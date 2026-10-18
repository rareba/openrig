import { validateAzurePoolConfiguration } from './azure-elastic-fleet.js';
import { withdrawalRequestSchema, continuationTargetSchema } from '../domain/elastic-continuation.js';
import type Database from 'better-sqlite3';
import { createHash, createPublicKey, randomBytes, randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { z } from 'zod';
import type { FleetMutation, FleetOperation } from '../domain/elastic-fleet.js';
import { sealCheckpointCapability } from '../domain/elastic-enrollment.js';
import type { ElasticFleetProjection } from '../domain/elastic-fleet-projection.js';
import type { AzureArgvRunner, AzureMutationContext, AzureNativeContext, AzureRemovalSeal } from './azure-elastic-fleet.js';
import type { AzureWorkerCommandClient, PinnedWorkerScript, TrustedWorkerEndpoint } from './azure-worker-command.js';
import { CheckpointCustody } from './elastic-checkpoint-custody.js';
import { finalBindingSchema } from './elastic-checkpoint-contract.js';
import { drainReceiptSchema, exportReceiptSchema, finalStatusReceiptSchema, removalSealSchema } from './elastic-lifecycle-contract.js';
import { ElasticLifecycleJournal, type LifecycleCommand, type LifecycleState } from './elastic-lifecycle-journal.js';
import { enrollmentDigest, type ElasticWorkerAttempt, type ElasticWorkerAttemptStore } from './elastic-worker-attempts.js';
import type { ElasticWorkerEnrollment } from './elastic-worker-enrollment.js';
import type { ElasticNativeOwner } from './elastic-native-owner.js';
import type { ManagedWorkerOperation } from './elastic-managed-operation.js';
import { retireElasticRegistration } from './elastic-retire-registration.js';

function refuse(reason = 'elastic_lifecycle_retained'): never { throw Error(reason); }
export interface ElasticManagedLifecycleOptions {
  db: Database.Database; owner: ElasticNativeOwner; store: ElasticWorkerAttemptStore; commands: AzureWorkerCommandClient;
  enrollment: ElasticWorkerEnrollment; custody: CheckpointCustody; projection: ElasticFleetProjection;
  manifestRaw: string; helperScript: PinnedWorkerScript; registryPath: string;
  now?: () => number; monotonic?: () => number;
}
/** Concrete root-command and retained-custody authority. Operators cannot supply receipts or clean flags. */
export class ElasticManagedLifecycle {
  readonly journal: ElasticLifecycleJournal;
  private readonly now: () => number;
  private readonly monotonic: () => number;
  private fresh = new Map<string, { sealDigest: string; stateDigest: string; wallStart: number; monoStart: number; revision: number }>();
  constructor(private readonly options: ElasticManagedLifecycleOptions) {
    this.journal = new ElasticLifecycleJournal(options.store, options.owner);
    this.now = options.now ?? Date.now;
    this.monotonic = options.monotonic ?? (() => performance.now());
  }
  private attempt(workerId: string | null): ElasticWorkerAttempt {
    const matches = this.options.store.list().filter(a => a.workerId === workerId);
    if (matches.length !== 1 || !matches[0]?.registration) refuse();
    return matches[0];
  }
  private authority(operation: FleetMutation): number {
    this.options.owner.assertHeld('fleet');
    const row = z.object({ revision: z.number().int(), paused: z.number().int(), config: z.string(), reconcile_owner: z.string() }).parse(
      this.options.db.prepare('SELECT revision,paused,config,reconcile_owner FROM elastic_fleet_config WHERE id=1').get());
    const association = this.options.db.prepare("SELECT owner,identity FROM elastic_native_owners WHERE scope='fleet-reconcile'").get() as { owner: string; identity: string } | undefined;
    if (row.paused || !operation.policy.enabled || operation.policy.mode !== 'active'
      || enrollmentDigest(JSON.parse(row.config)) !== enrollmentDigest(operation.configuration)
      || association?.owner !== row.reconcile_owner || association.identity !== JSON.stringify(this.options.owner.identity)) refuse();
    return row.revision;
  }
  private obligations(a: ElasticWorkerAttempt): void {
    if (this.options.db.prepare("SELECT 1 FROM elastic_human_proxies WHERE worker_id=? AND state NOT IN ('settled','superseded') LIMIT 1").get(a.workerId))
      refuse('elastic_owned_continuation_required');
  }
  private commandAttempt(a: ElasticWorkerAttempt, command: LifecycleCommand): ElasticWorkerAttempt {
    return { ...a, command: { name: command.name, requestDigest: command.requestDigest, operation: command.operation, state: command.state } };
  }
  private endpoint(a: ElasticWorkerAttempt): TrustedWorkerEndpoint {
    return z.object({ instance: z.object({ version: z.literal(1), imageReference: z.string(), resourceId: z.string(), vmId: z.string(), manifestDigest: z.string(), fleetEpoch: z.string(), operationId: z.string(), privateIPv4: z.string() }).strict(),
      baseUrl: z.string(), location: z.string() }).strict().parse(this.options.store.readPrivateInput(a.id, 'trusted-endpoint'));
  }
  private async submit(a: ElasticWorkerAttempt, context: AzureMutationContext, operation: ManagedWorkerOperation, payload: unknown): Promise<LifecycleCommand> {
    this.authority(context.operation);
    const identity = this.options.commands.commandIdentity(a, operation, payload, this.options.helperScript);
    const inputName = `life-input-${identity.requestDigest}`;
    const saved = this.options.store.savePrivateInput(a.id, inputName, payload);
    if (enrollmentDigest(saved) !== enrollmentDigest(payload)) refuse();
    return { name: identity.name, operation, requestDigest: identity.requestDigest, state: 'intent', inputName };
  }
  private async dispatch(a: ElasticWorkerAttempt, context: AzureMutationContext, command: LifecycleCommand): Promise<LifecycleCommand> {
    this.authority(context.operation);
    if(command.operation!=='withdraw-drain' && command.operation!=='finish-continuation')this.obligations(a);
    const state = await this.options.commands.submitManagedCommand(this.commandAttempt(a, command), command.operation, {
      configuration: context.configuration, endpoint: this.endpoint(a), script: this.options.helperScript,
      payload: this.options.store.readPrivateInput(a.id, command.inputName),
      assertDispatch: () => { this.authority(context.operation); if(command.operation!=='withdraw-drain' && command.operation!=='finish-continuation')this.obligations(a); },
    });
    return { ...command, state };
  }
  private preserveOutput(a: ElasticWorkerAttempt, command: LifecycleCommand, output: string): LifecycleCommand {
    const outputDigest = createHash('sha256').update(output).digest('hex');
    const saved = this.options.store.savePrivateInput(a.id, `life-output-${command.requestDigest}`, output);
    if (saved !== output || command.outputDigest && command.outputDigest !== outputDigest) refuse('elastic_root_response_changed');
    return { ...command, state: 'succeeded', outputDigest };
  }
  private async advanceCommand(a: ElasticWorkerAttempt, context: AzureMutationContext, state: LifecycleState, operation: ManagedWorkerOperation, payload: unknown,
    decode: (raw: unknown) => unknown): Promise<{ state: LifecycleState; completed: boolean }> {
    const command = state.commands.find(c => c.operation === operation);
    if (!command) {
      const next = await this.submit(a, context, operation, payload);
      state = this.journal.advance(state, { commands: [...state.commands, next] });
      const submitted = await this.dispatch(a, context, next);
      return { state: this.journal.advance(state, { commands: [...state.commands.slice(0, -1), submitted] }), completed: false };
    }
    const expected = this.options.commands.commandIdentity(a, operation, this.options.store.readPrivateInput(a.id, command.inputName), this.options.helperScript);
    if (expected.name !== command.name || expected.requestDigest !== command.requestDigest) refuse();
    if (command.state === 'failed') refuse('elastic_root_phase_failed');
    if (command.state === 'succeeded') return { state, completed: true };
    const result = await this.options.commands.inspectManagedCommand(this.commandAttempt(a, command), context.configuration);
    if (result.state === 'pending' || result.state === 'unknown') return { state, completed: false };
    const updated: LifecycleCommand = result.state === 'succeeded' ? { ...this.preserveOutput(a, command, result.output), result: decode(JSON.parse(result.output)) }
      : { ...command, state: 'failed' };
    state = this.journal.advance(state, { commands: state.commands.map(c => c.name === command.name ? updated : c) });
    if (updated.state === 'failed') refuse('elastic_root_phase_failed');
    return { state, completed: true };
  }
  async beginDrain(context: AzureMutationContext): Promise<void> {
    this.authority(context.operation);
    const a = this.attempt(context.operation.workerId);
    this.obligations(a);
    let state = this.journal.begin(a.id, context.operation.operationId);
    if (state.drain) return;
    if (!state.commands.length) {
      const observed = await this.options.enrollment.observeManagedWorker(a, context.configuration);
      if (observed.worker.active !== 0 || !observed.drainEligible || !observed.probe?.ledger.clean || !observed.probe.process.complete || !observed.probe.process.idle) refuse('elastic_owned_continuation_required');
    }
    const result = await this.advanceCommand(a, context, state, 'begin-drain', {
      identity: { workerId: a.workerId, vmId: a.binding.vmId, manifestDigest: a.binding.manifestDigest }, operationId: context.operation.operationId,
    }, raw => {
      const receipt = drainReceiptSchema.parse(raw);
      if (receipt.workerId !== a.workerId || receipt.vmId !== a.binding.vmId || receipt.manifestDigest !== a.binding.manifestDigest
        || receipt.drainOperationId !== context.operation.operationId) refuse();
      return receipt;
    });
    state = result.state;
    if (!result.completed) refuse('elastic_root_phase_pending');
    this.journal.advance(state, { drain: drainReceiptSchema.parse(state.commands.find(c => c.operation === 'begin-drain')?.result) });
  }
  private bindRemoval(a: ElasticWorkerAttempt, context: AzureMutationContext): LifecycleState {
    let state = this.journal.read(a.id);
    if (!state?.drain) refuse();
    const binding = finalBindingSchema.parse({ workerId: a.workerId, vmId: a.binding.vmId, manifestDigest: a.binding.manifestDigest,
      fleetEpoch: a.binding.fleetEpoch, drainGeneration: state.drain.generation, removalOperationId: state.beginOperationId });
    if (state.binding) {
      if (enrollmentDigest(state.binding) !== enrollmentDigest(binding) || state.removalOperationId !== context.operation.operationId || state.operationRevision !== context.operation.revision
        || state.configurationDigest !== enrollmentDigest(context.operation.configuration)) refuse();
    } else state = this.journal.advance(state, { binding, removalOperationId: context.operation.operationId, operationRevision: context.operation.revision, configurationDigest: enrollmentDigest(context.operation.configuration) });
    return state;
  }
  async hasOwnedContinuation(workerId:string,configuration:import('../domain/elastic-fleet.js').ElasticFleetConfiguration):Promise<boolean> {
    this.options.owner.assertHeld('fleet');
    const a=this.attempt(workerId),state=this.journal.read(a.id);
    if(!state?.drain||state.withdrawal||state.deleteIntent||state.commands.some(c=>c.operation==='quiesce'))return false;
    const provider=validateAzurePoolConfiguration(configuration.provider,configuration.policy.poolId);
    const view=z.object({state:z.object({generation:z.number(),drainOperationId:z.string().nullable()}),candidates:z.array(z.object({outboxId:z.string(),queueDigest:z.string(),target:continuationTargetSchema}))})
      .parse(await this.options.enrollment.continuationRequest(a,provider,'GET'));
    return view.state.generation===state.drain.generation&&view.state.drainOperationId===state.beginOperationId&&view.candidates.length>0;
  }
  private async withdrawForContinuation(a:ElasticWorkerAttempt,context:AzureMutationContext,state:LifecycleState):Promise<void> {
    if(state.deleteIntent || state.commands.some(c=>c.operation==='quiesce'))refuse('elastic_quiesce_already_intended');
    if(!state.withdrawal) {
      const view=z.object({state:z.object({generation:z.number(),drainOperationId:z.string().nullable()}),candidates:z.array(z.object({outboxId:z.string(),queueDigest:z.string(),target:continuationTargetSchema}))})
        .parse(await this.options.enrollment.continuationRequest(a,context.configuration,'GET'));
      const candidate=view.candidates[0];
      if(!candidate || view.state.generation!==state.drain?.generation || view.state.drainOperationId!==state.beginOperationId)refuse('elastic_owned_continuation_unavailable');
      const request=withdrawalRequestSchema.parse({identity:{workerId:a.workerId,vmId:a.binding.vmId,manifestDigest:a.binding.manifestDigest},generation:view.state.generation,
        operationId:state.beginOperationId,withdrawalId:randomUUID(),...candidate});
      state=this.journal.advance(state,{withdrawal:{request,receipt:null,finished:false}});
    }
    const w=state.withdrawal!;
    if(!w.receipt) {
      const result=await this.advanceCommand(a,context,state,'withdraw-drain',w.request,raw=>{
        const receipt=z.object({withdrawalId:z.string().uuid(),generation:z.number().int().positive(),actionId:z.string().uuid()}).strict().parse(raw);
        if(receipt.withdrawalId!==w.request.withdrawalId||receipt.generation!==w.request.generation+1)refuse();return receipt;
      });
      if(!result.completed)refuse('elastic_withdrawal_pending');
      const receipt=z.object({withdrawalId:z.string().uuid(),generation:z.number().int().positive(),actionId:z.string().uuid()}).strict()
        .parse(result.state.commands.find(c=>c.operation==='withdraw-drain')?.result);
      state=this.journal.advance(result.state,{withdrawal:{...w,receipt}});
      this.options.db.transaction(()=>{
        this.options.db.prepare("UPDATE elastic_fleet_drains SET state='continuation',updated_at=? WHERE worker_id=? AND begin_operation_id=?").run(this.now(),a.workerId,state.beginOperationId);
        this.options.db.prepare('DELETE FROM elastic_idle_windows WHERE worker_id=?').run(a.workerId);
      }).immediate();
    }
    refuse('elastic_removal_withdrawn');
  }
  /** Bounded progress of quarantined native lineages while the fleet can work on other workers. */
  async reconcileContinuations(configuration:import('../domain/elastic-fleet.js').ElasticFleetConfiguration):Promise<void> {
    this.options.owner.assertHeld('fleet');
    for(const a of this.options.store.list()) {
      let state=this.journal.read(a.id);const w=state?.withdrawal;
      if(!state||!w?.receipt||w.finished||!state.removalOperationId)continue;
      const row=this.options.db.prepare('SELECT * FROM elastic_fleet_operations WHERE operation_id=?').get(state.removalOperationId) as {operation_id:string;type:'remove';state:'withdrawn';worker_id:string;count:number;target_capacity:number;revision:number;snapshot:string;configuration:string;error:string|null;created_at:number;updated_at:number}|undefined;
      if(!row||row.state!=='withdrawn'||enrollmentDigest(JSON.parse(row.configuration))!==enrollmentDigest(configuration))continue;
      const operation: FleetMutation={operationId:row.operation_id,type:row.type,state:row.state,workerId:row.worker_id,count:row.count,targetCapacity:row.target_capacity,
        revision:row.revision,snapshot:JSON.parse(row.snapshot),error:row.error,createdAt:row.created_at,updatedAt:row.updated_at,configuration,policy:configuration.policy};
      try {
        this.authority(operation);
        const provider=validateAzurePoolConfiguration(configuration.provider,configuration.policy.poolId);
        if(!state.commands.some(c=>c.operation==='finish-continuation'))await this.options.enrollment.continuationRequest(a,provider,'POST',{withdrawalId:w.request.withdrawalId});
        const observed=await this.options.enrollment.observeManagedWorker(a,provider);
        if(!observed.probe?.ledger.clean||!observed.probe.process.complete||!observed.probe.process.idle)continue;
        const context:AzureMutationContext={operation,configuration:provider,instance:{resourceId:a.identity.childResourceId,instanceId:a.identity.instanceId,vmId:a.binding.vmId,workerId:a.workerId,protected:true,provisioningState:'Succeeded'}};
        const result=await this.advanceCommand(a,context,state,'finish-continuation',{identity:{workerId:a.workerId,vmId:a.binding.vmId,manifestDigest:a.binding.manifestDigest},generation:w.receipt.generation},raw=>{
          const value=z.object({generation:z.number().int().positive(),retired:z.literal(true),dispositionDigest:z.string()}).strict().parse(raw);
          if(value.generation!==w.receipt!.generation)refuse();return value;
        });
        if(!result.completed)continue;
        state=this.journal.advance(result.state,{withdrawal:{...w,finished:true}});
        this.options.db.prepare("UPDATE elastic_fleet_drains SET state='continuation-finished',updated_at=? WHERE worker_id=? AND begin_operation_id=?").run(this.now(),a.workerId,state.beginOperationId);
        this.options.db.prepare('DELETE FROM elastic_idle_windows WHERE worker_id=?').run(a.workerId);
      } catch { /* Native unknown effects retain this worker; continue disjoint lineages. */ }
    }
  }
  async sealForRemoval(context: AzureMutationContext): Promise<AzureRemovalSeal> {
    this.authority(context.operation);
    const a = this.attempt(context.operation.workerId);
    let state = this.bindRemoval(a, context);
    if (state.deleteIntent) refuse('elastic_delete_reconciliation_required');
    if(state.withdrawal){await this.withdrawForContinuation(a,context,state);refuse();}
    const binding = state.binding!;
    const done = (operation: ManagedWorkerOperation) => state.commands.some(c => c.operation === operation && c.state === 'succeeded');
    if (!done('quiesce')) {
      if (!state.commands.some(c => c.operation === 'quiesce')) {
        const observed = await this.options.enrollment.observeManagedWorker(a, context.configuration);
        if (!observed.probe || observed.probe.state.phase !== 'draining' || observed.probe.state.generation !== binding.drainGeneration
          || observed.probe.state.drainOperationId !== state.beginOperationId || !observed.drainEligible || !observed.probe.ledger.clean
          || !observed.probe.process.complete || !observed.probe.process.idle) {await this.withdrawForContinuation(a,context,state);refuse();}
      }
      await this.advanceCommand(a, context, state, 'quiesce', { binding }, raw => {
        const parsed = z.object({ quiesced: z.literal(true), binding: finalBindingSchema }).strict().parse(raw);
        if (enrollmentDigest(parsed.binding) !== enrollmentDigest(binding)) refuse();
        return parsed;
      });
      refuse('elastic_root_phase_pending');
    }
    if (!done('final-checkpoint-export')) {
      await this.advanceCommand(a, context, state, 'final-checkpoint-export', { binding }, raw => {
        const receipt = exportReceiptSchema.parse(raw);
        if (receipt.uploaded || enrollmentDigest({ binding, manifestDigest: receipt.manifestDigest }) !== receipt.checkpointId) refuse();
        return receipt;
      });
      refuse('elastic_root_phase_pending');
    }
    if (!state.exported) state = this.journal.advance(state, { exported: exportReceiptSchema.parse(state.commands.find(c => c.operation === 'final-checkpoint-export')?.result) });
    const { uploaded: _uploaded, ...summary } = state.exported!;
    if (!done('final-checkpoint-upload')) {
      const material = this.options.store.custodyMaterial(a.id);
      let attemptId: string;
      try { attemptId = this.options.custody.registerAttempt({ ...material, trust: { manifestRaw: this.options.manifestRaw, instance: this.endpoint(a).instance } }).attemptId; }
      finally { material.controllerPrivateKeyPem.fill(0); }
      const uploadInput=`life-upload-${enrollmentDigest(state.beginOperationId)}`;
      let payload = this.options.store.readPrivateInput(a.id, uploadInput);
      if (!payload) {
        if (!a.workerPublicKeyPem) refuse();
        const capability = this.options.custody.reserve({ attemptId, binding, summary, maxBytes: summary.ciphertextBytes, expiresAt: this.now() + 3600000 });
        const plaintext = Buffer.from(JSON.stringify(capability));
        try { payload = this.options.store.savePrivateInput(a.id, uploadInput, { binding,
          capabilityEnvelope: sealCheckpointCapability(plaintext, a.binding, createPublicKey(a.workerPublicKeyPem)) }); }
        finally { plaintext.fill(0); }
      }
      await this.advanceCommand(a, context, state, 'final-checkpoint-upload', payload, raw => {
        const receipt = exportReceiptSchema.parse(raw);
        if (!receipt.uploaded || enrollmentDigest({ ...receipt, uploaded: false }) !== enrollmentDigest(state.exported)) refuse();
        return receipt;
      });
      refuse('elastic_root_phase_pending');
    }
    if (!state.checkpointReceiptDigest) {
      const receipt = this.options.custody.verify(summary.checkpointId);
      state = this.journal.advance(state, { checkpointReceiptDigest: enrollmentDigest(receipt) });
    }
    const receipt = this.options.custody.receipt(summary.checkpointId);
    if (!receipt || enrollmentDigest(receipt) !== state.checkpointReceiptDigest) refuse();
    if (!done('final-checkpoint-ack')) {
      await this.advanceCommand(a, context, state, 'final-checkpoint-ack', { binding, receipt }, raw => this.exactSeal(raw, state));
      refuse('elastic_root_phase_pending');
    }
    return this.finalize(a, context, state);
  }
  private exactSeal(raw: unknown, state: LifecycleState): AzureRemovalSeal {
    const seal = removalSealSchema.parse(raw), b = state.binding;
    if (!b || seal.workerId !== b.workerId || seal.vmId !== b.vmId || seal.operationId !== b.removalOperationId
      || seal.drainGeneration !== String(b.drainGeneration) || seal.drainConfirmedAt !== state.drain?.drainStartedAt
      || seal.observedAt <= seal.drainConfirmedAt) refuse();
    return seal;
  }
  private async finalize(a: ElasticWorkerAttempt, context: AzureMutationContext, state: LifecycleState): Promise<AzureRemovalSeal> {
    const prior = state.rounds.at(-1);
    if (prior && prior.disposition === 'pending') {
      const observed = await this.options.commands.inspectManagedCommand(this.commandAttempt(a, prior.command), context.configuration);
      if (observed.state === 'pending' || observed.state === 'unknown') refuse('elastic_status_pending');
      if (observed.state !== 'succeeded') refuse('elastic_status_failed');
      const receipt = this.statusReceipt(JSON.parse(observed.output), prior, state);
      this.journal.advance(state, { rounds: [...state.rounds.slice(0, -1), { ...prior, receipt, command: this.preserveOutput(a, prior.command, observed.output), disposition: 'stale' }] });
      refuse('elastic_status_previous_turn');
    }
    const wallStart = this.now(), monoStart = this.monotonic(), revision = this.authority(context.operation);
    const round = (prior?.round ?? 0) + 1, nonce = randomBytes(32).toString('hex');
    const command = await this.submit(a, context, 'seal-status', { binding: state.binding, round, nonce });
    let current: LifecycleState['rounds'][number] = { round, nonce, ownerDigest: enrollmentDigest(this.options.owner.claimIdentity('fleet')), revision, command, receipt: null, disposition: 'pending' };
    state = this.journal.advance(state, { rounds: [...state.rounds, current] });
    current = { ...current, command: await this.dispatch(a, context, command) };
    state = this.journal.advance(state, { rounds: [...state.rounds.slice(0, -1), current] });
    // One readback in the issuing turn. An incomplete ARM result retains the original command.
    const observed = await this.options.commands.inspectManagedCommand(this.commandAttempt(a, current.command), context.configuration);
    if (observed.state !== 'succeeded') refuse('elastic_status_pending');
    const receipt = this.statusReceipt(JSON.parse(observed.output), current, state);
    current = { ...current, receipt, command: this.preserveOutput(a, current.command, observed.output), disposition: 'completed' };
    state = this.journal.advance(state, { rounds: [...state.rounds.slice(0, -1), current] });
    // Root custody keeps its original begin-drain identity. The immutable journal binds the separate coordinator removal.
    if (state.removalOperationId !== context.operation.operationId) refuse();
    const removalSeal = { ...receipt.seal, operationId: state.removalOperationId };
    this.fresh.set(a.id, { sealDigest: enrollmentDigest(removalSeal), stateDigest: enrollmentDigest(state), wallStart, monoStart, revision });
    this.checkFresh(a, context.operation, removalSeal, state);
    return removalSeal;
  }
  private statusReceipt(raw: unknown, round: LifecycleState['rounds'][number], state: LifecycleState) {
    const receipt = finalStatusReceiptSchema.parse(raw);
    if (receipt.round !== round.round || receipt.nonce !== round.nonce || enrollmentDigest(receipt.binding) !== enrollmentDigest(state.binding)
      || receipt.checkpointReceiptDigest !== state.checkpointReceiptDigest) refuse();
    this.exactSeal(receipt.seal, state);
    return receipt;
  }
  private checkFresh(a: ElasticWorkerAttempt, operation: FleetMutation, seal: AzureRemovalSeal, state: LifecycleState): void {
    this.obligations(a);
    if (!state.exported || enrollmentDigest(this.options.custody.receipt(state.exported.checkpointId)) !== state.checkpointReceiptDigest) refuse();
    if (state.removalOperationId !== operation.operationId || seal.operationId !== operation.operationId) refuse();
    const revision = this.authority(operation), fresh = this.fresh.get(a.id);
    if (!fresh || fresh.sealDigest !== enrollmentDigest(seal) || fresh.stateDigest !== enrollmentDigest(state)
      || revision !== fresh.revision || state.rounds.at(-1)?.ownerDigest !== enrollmentDigest(this.options.owner.claimIdentity('fleet'))) refuse('elastic_status_expired');
    const now = this.now(), mono = this.monotonic();
    if (seal.observedAt < fresh.wallStart || seal.observedAt > now || now < fresh.wallStart || now - seal.observedAt > 5000
      || mono < fresh.monoStart || mono - fresh.monoStart > 5000) refuse('elastic_status_expired');
  }
  /** Called only after the final full ARM target read. Persistence occurs before the final synchronous check. */
  dispatchDelete(context: AzureMutationContext, seal: AzureRemovalSeal, argv: readonly string[], limits: { timeout: number; maxBuffer: number }, run: AzureArgvRunner): Promise<string> {
    const a = this.attempt(context.operation.workerId);
    let state = this.journal.read(a.id);
    const expected = ['vmss', 'delete-instances', '--resource-group', context.configuration.resourceGroup, '--name', context.configuration.scaleSetName,
      '--instance-ids', a.identity.instanceId, '--subscription', context.configuration.subscription, '--only-show-errors', '--output', 'json'];
    if (!state || state.deleteIntent || context.instance.resourceId !== a.identity.childResourceId || context.instance.instanceId !== a.identity.instanceId
      || context.instance.vmId !== a.binding.vmId || enrollmentDigest(argv) !== enrollmentDigest(expected)) refuse();
    this.checkFresh(a, context.operation, seal, state);
    const round = state.rounds.at(-1)!;
    const oldFresh = this.fresh.get(a.id)!;
    state = this.journal.advance(state, { deleteIntent: { operationId: context.operation.operationId, revision: oldFresh.revision,
      resourceId: context.instance.resourceId, instanceId: context.instance.instanceId, vmId: context.instance.vmId,
      round: round.round, requestDigest: round.command.requestDigest, receiptDigest: enrollmentDigest(round.receipt), ownerDigest: round.ownerDigest } });
    this.fresh.set(a.id, { ...oldFresh, stateDigest: enrollmentDigest(state) });
    this.checkFresh(a, context.operation, seal, state);
    // No asynchronous gap between final authority/freshness check and trusted argv dispatch.
    return run(argv, { ...limits, beforeDispatch: () => this.checkFresh(a, context.operation, seal, state!) });
  }
  async retireRegistration(context: AzureMutationContext): Promise<void> {
    this.options.owner.assertHeld('fleet');
    const a = this.attempt(context.operation.workerId);
    let state = this.journal.read(a.id);
    const intent = state?.deleteIntent;
    if (!state || !intent || state.binding?.removalOperationId !== state.beginOperationId || state.drain?.drainOperationId !== state.beginOperationId
      || state.removalOperationId !== context.operation.operationId || intent.operationId !== context.operation.operationId || intent.vmId !== context.instance.vmId
      || intent.instanceId !== context.instance.instanceId || intent.resourceId !== context.instance.resourceId) refuse();
    if (!state.deleteConfirmed) state = this.journal.advance(state, { deleteConfirmed: true });
    retireElasticRegistration(this.options.store, a, this.options.registryPath);
    this.options.projection.retire(a);
    if (!state.retired) this.journal.advance(state, { retired: true });
    this.fresh.delete(a.id);
  }
  async retireAbsent(operation: FleetMutation, configuration: AzureMutationContext['configuration'], instances: AzureNativeContext['instances']): Promise<boolean> {
    this.options.owner.assertHeld('fleet');
    const a = this.attempt(operation.workerId), state = this.journal.read(a.id), intent = state?.deleteIntent;
    if (!intent) return false;
    if (intent.operationId !== operation.operationId || instances.some(i => i.vmId === intent.vmId || i.instanceId === intent.instanceId)) refuse('elastic_delete_unknown');
    await this.retireRegistration({ operation, configuration, instance: { resourceId: intent.resourceId, instanceId: intent.instanceId,
      vmId: intent.vmId, workerId: a.workerId, protected: true, provisioningState: 'Succeeded' } });
    return true;
  }
  async inspectNative(context: AzureNativeContext & { operation: FleetOperation }): Promise<'pending' | 'unknown' | 'succeeded' | 'withdrawn'> {
    const a = this.attempt(context.operation.workerId), state = this.journal.read(a.id);
    if (!state) return context.operation.type === 'beginDrain' ? 'pending' : 'unknown';
    if(context.operation.type==='remove'&&state.removalOperationId===context.operation.operationId&&state.withdrawal?.receipt)return 'withdrawn';
    if (context.operation.type === 'beginDrain') return state.drain?.drainOperationId === context.operation.operationId ? 'succeeded' : this.inspectLast(a, state, context.configuration);
    if (state.removalOperationId !== context.operation.operationId) return state.binding ? 'unknown' : 'pending';
    if (state.deleteIntent) {
      if (context.instances.some(i => i.vmId === state.deleteIntent!.vmId || i.instanceId === state.deleteIntent!.instanceId)) return 'unknown';
      return state.retired ? 'succeeded' : 'pending';
    }
    return this.inspectLast(a, state, context.configuration);
  }
  private async inspectLast(a: ElasticWorkerAttempt, state: LifecycleState, configuration: AzureMutationContext['configuration']): Promise<'pending' | 'unknown'> {
    const command = state.rounds.at(-1)?.command ?? state.commands.at(-1);
    if (!command || command.state === 'succeeded') return 'pending';
    if (command.state === 'failed') return 'unknown';
    const observed = await this.options.commands.inspectManagedCommand(this.commandAttempt(a, command), configuration);
    return observed.state === 'succeeded' || observed.state === 'pending' ? 'pending' : 'unknown';
  }
}
