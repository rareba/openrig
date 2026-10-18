import { QueueRepository } from '../src/domain/queue-repository.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { EventBus } from '../src/domain/event-bus.js';
import { issueWithdrawalAction, nativeContinuation, type ContinuationTarget } from '../src/domain/elastic-continuation.js';
import { afterEach, expect, it } from 'vitest';
import { mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync, createReadStream } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { generateKeyPairSync } from 'node:crypto';
import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { ElasticNativeOwner } from '../src/adapters/elastic-native-owner.js';
import { ElasticWorkerAttemptStore, enrollmentDigest } from '../src/adapters/elastic-worker-attempts.js';
import { ElasticWorkerEnrollment, managedHostId } from '../src/adapters/elastic-worker-enrollment.js';
import { AzureWorkerCommandClient } from '../src/adapters/azure-worker-command.js';
import { createAzureElasticFleetIO } from '../src/adapters/azure-elastic-fleet.js';
import { ElasticManagedLifecycle } from '../src/adapters/elastic-managed-lifecycle.js';
import { ElasticFleetProjection } from '../src/domain/elastic-fleet-projection.js';
import { ElasticWorkerControl } from '../src/domain/elastic-worker-control.js';
import { CheckpointCustody } from '../src/adapters/elastic-checkpoint-custody.js';
import { captureFinalCheckpoint, encryptCheckpoint, sha256, type FinalBinding, type EncryptedCheckpoint } from '../src/adapters/elastic-worker-checkpoint.js';
import { parseHelperPayload } from '../src/adapters/elastic-worker-helper.js';
import { parseLinuxManifest } from '../src/adapters/linux-privileged-worker-boundary.js';
import { openCheckpointCapability } from '../src/domain/elastic-enrollment.js';
import { verifyCustodyReceipt, type SignedCustodyReceipt } from '../src/adapters/elastic-checkpoint-upload.js';
import { ElasticFleetService, type FleetMutation } from '../src/domain/elastic-fleet.js';
import type { AzureRemovalSeal } from '../src/adapters/azure-elastic-fleet.js';
const cleanup: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });
async function fixture() {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'elastic-lifecycle-')));
  cleanup.push(() => rmSync(root, { recursive: true, force: true }));
  for (const name of ['attempts', 'commands', 'custody', 'work', 'control']) mkdirSync(join(root, name), { mode: 0o700 });
  const dbPath = join(root, 'coordinator.sqlite'), db = createDb(dbPath);
  const workerPath = join(root, 'work/native.sqlite'), worker = createDb(workerPath);
  migrate(db, ALL_MIGRATIONS); migrate(worker, ALL_MIGRATIONS);
  cleanup.push(() => { db.close(); worker.close(); });
  let clock = Date.now();
  const native = new ElasticWorkerControl(worker, () => clock);
  const queue=new QueueRepository(worker,new EventBus(worker));queue.attachOutbox(new OutboxHandler(worker));
  let lateHuman=false,question:Awaited<ReturnType<typeof queue.create>>|undefined,replyId:string|undefined;
  const continuationTarget:ContinuationTarget={nodeId:'node',session:'worker-a@elastic',occupant:'occupant',pane:'%1',conversation:'conversation',sessionFile:'/fixture/session.jsonl',stateRoot:'/fixture/state/pi',cwd:'/fixture',model:null,launchId:'launch',process:{bootId:'boot',runnerPid:10,childPid:11,runnerStartTime:'a',childStartTime:'b'}};

  const raw = JSON.parse(readFileSync(new URL('../../../integrations/azure-elastic/manifest.example.json', import.meta.url), 'utf8'));
  raw.pins.forEach((p: { sha256: string }) => p.sha256 = 'a'.repeat(64));
  raw.resourceId = '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture';
  raw.linux.imageReference = 'Canonical:ubuntu:24:1.2.3';
  raw.ownedRoots = [join(root, 'work')]; raw.controlDirectory = join(root, 'control');
  raw.linux.home = join(root, 'work/home'); raw.linux.stateRoot = join(root, 'work/home/state/pi');
  raw.linux.database = workerPath; raw.linux.databases = [workerPath]; raw.linux.temp = join(root, 'work/temp');
  const manifestRaw = JSON.stringify(raw), manifest = parseLinuxManifest(manifestRaw), manifestDigest = sha256(manifestRaw);
  const parts = manifest.resourceId.split('/');
  const configuration = { kind: 'azure-vmss' as const, subscription: parts[2]!, resourceGroup: parts[4]!, scaleSetName: parts[8]!, resourceId: manifest.resourceId,
    poolId: 'fixture', ownershipTag: { key: 'pool', value: 'fixture' }, image: { kind: 'marketplace' as const, publisher: 'Canonical', offer: 'ubuntu-24_04-lts', sku: 'server', version: '24.04.202607080' }, bootstrap: { path: '/etc/openrig-elastic/manifest.json', digest: manifestDigest } };
  const image = manifest.linux.imageReference.split(':');
  Object.assign(configuration.image, { publisher: image[0], offer: image[1], sku: image[2], version: image[3] });
  const policy = { poolId: 'fixture', enabled: true, mode: 'active' as const, maxWorkers: 1, slotsPerWorker: 2, capabilities: ['github:1:public-read'], pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900 };
  const fleetConfig = { policy, provider: configuration };
  db.prepare('UPDATE elastic_fleet_config SET config=?,revision=1').run(JSON.stringify(fleetConfig));
  const owner = new ElasticNativeOwner(db, dbPath), store = new ElasticWorkerAttemptStore(join(root, 'attempts'), owner);
  const plan = { version: 1 as const, operationId: 'prepare-1', manifestDigest, specDigest: 'b'.repeat(64), model: 'fixture/model', approvedActionKeys: [], repositoryAccess: { mode: 'public-readonly' as const },
    seats: ['worker-a@elastic', 'worker-b@elastic'].map(sessionName => ({ sessionName, repository: { id: 1, name: 'fixture/repo', baseRef: 'refs/heads/main', sha: 'a'.repeat(40) } })), owner: null };
  const script = { path: join(root, 'script.py'), sha256: sha256('print("fixed")') }; writeFileSync(script.path, 'print("fixed")', { mode: 0o600 });
  const artifact = { version: 1 as const, url: 'https://10.0.0.1:8443/api/elastic-artifacts/' + 'd'.repeat(64), privateIPv4: '10.0.0.1', archiveSha256: 'd'.repeat(64), archiveBytes: 1024, caPem: 'fixture', caSha256: 'e'.repeat(64) };
  const recipe = { distribution: 'ubuntu' as const, release: '24.04' as const, architecture: 'amd64' as const, basePackages: {}, packages: [], finalTools: [], signedMetadata: { keyringFile: 'fixture', keyringSha256: 'e'.repeat(64), releases: [] } };
  const vmId = '11111111-1111-4111-8111-111111111111', childId = `${manifest.resourceId}/virtualMachines/0`;
  const workerKey = generateKeyPairSync('rsa', { modulusLength: 3072 });
  const configurationDigest = enrollmentDigest({ manifest: manifestDigest, plan, artifact, recipe, stage: script, helper: script, checkpointCaPem: 'fixture', epoch: 'epoch', commit: 'c'.repeat(40) });
  const a = await owner.run('fleet', async () => {
    const seeded = store.createOrRead({ resourceId: manifest.resourceId, childResourceId: childId, instanceId: '0', vmId, operationId: 'provision-1', fleetEpoch: 'epoch', manifestDigest,
      artifactDigest: artifact.archiveSha256, specDigest: plan.specDigest, checkoutDigest: enrollmentDigest(plan.seats), configurationDigest });
    store.saveBearer(seeded.id, 'fixture-bearer-'.repeat(4));
    return store.advance(seeded.id, seeded.revision, { phase: 'ready', workerPublicKeyPem: workerKey.publicKey.export({ format: 'pem', type: 'spki' }).toString(),
      registration: { hostId: managedHostId(seeded.workerId), selfHostId: 'worker-host', baseUrl: `http://10.0.0.2:${manifest.port}` } });
  });
  native.enroll({ workerId: a.workerId, vmId, manifestDigest });
  const endpoint = { instance: { version: 1 as const, imageReference: manifest.linux.imageReference, resourceId: manifest.resourceId, vmId, manifestDigest, fleetEpoch: 'epoch', operationId: 'provision-1', privateIPv4: '10.0.0.2' }, baseUrl: a.registration!.baseUrl, location: 'fixture' };
  store.savePrivateInput(a.id, 'trusted-endpoint', endpoint);
  const registry = join(root, 'hosts.yaml'); writeFileSync(registry, JSON.stringify({ hosts: [{ id: a.registration!.hostId, transport: 'http', hostId: 'worker-host', url: endpoint.baseUrl, bearer_file: store.tokenPath(a.id) }, { id: 'permanent', transport: 'http', url: 'http://10.0.0.3:7433' }] }), { mode: 0o600 });
  const custody = new CheckpointCustody({ directory: join(root, 'custody'), durableStoreId: 'fixture', retentionPolicyId: 'fixture', maxBytes: 16 * 1024 ** 2, maxReservedBytes: 32 * 1024 ** 2, maxStoredBytes: 128 * 1024 ** 2, maxCheckpoints: 4 });
  const operations: string[] = [], results = new Map<string, { digest: string; output: string }>();
  let stopped = false, encrypted: EncryptedCheckpoint | undefined, ack: SignedCustodyReceipt | undefined;
  let finalHook: (() => void) | undefined;
  let unknownDelete = false, dispatchDelay = false, replacement = false;
  let unknown = false, pauseBeforePut = false, mono = 100, deleted = false, slowFinal = false, rootStatusPending = false, lists = 0;
  const seal = (binding: FinalBinding): AzureRemovalSeal => ({ workerId: a.workerId, vmId, operationId: binding.removalOperationId, observedAt: ++clock,
    drainConfirmedAt: native.read()!.drainStartedAt!, drainGeneration: String(binding.drainGeneration), beginDrainConfirmed: true, fenced: true,
    active: 0, claims: 0, processes: 0, pendingTransfers: false, pendingActivationIntents: false, coordinatorCheckpointAcknowledged: true, clean: true, pendingHumanReturns: false });
  const imageReference = configuration.image;
  const run = async (args: readonly string[], bounds?: { beforeDispatch?: () => void }) => {
    if (args[1] === 'delete-instances' && dispatchDelay) mono += 5001;
    bounds?.beforeDispatch?.();
    if (args[0] === 'rest' && args.includes('put')) {
      const body = JSON.parse(readFileSync(args[args.indexOf('--body') + 1]!.slice(1), 'utf8'));
      const request = JSON.parse(Buffer.from(body.properties.protectedParameters[0].value, 'base64').toString());
      parseHelperPayload(request.operation, request.payload);
      expect(request.instance.operationId).toBe('provision-1');
      expect(args.join(' ')).not.toContain('capabilityEnvelope');
      operations.push(request.operation);
      const p = request.payload; let output: unknown;
      if (request.operation === 'begin-drain') {
        if(lateHuman&&!question)question=await queue.create({sourceSession:'worker-a@elastic',destinationSession:'human@kernel',body:'Decision',summary:'Decision',evidenceRef:'/fixture',humanIntent:'decision',nudge:false});
        output = native.beginDrain(p.identity, p.operationId);
      } else if(request.operation==='withdraw-drain') {
        const action=worker.transaction(()=>issueWithdrawalAction(worker,p,'existing-runner')).immediate();
        output={withdrawalId:p.withdrawalId,generation:action.generation,actionId:action.actionId};
      }
      else if(request.operation==='finish-continuation') {
        native.retireContinuation();output={generation:p.generation,retired:true,dispositionDigest:native.generation()!.disposition_digest};
      }
      else if (request.operation === 'quiesce') {
        native.sealAdmission(p.binding, p.binding.drainGeneration); stopped = true;
        worker.exec('CREATE TABLE shutdown_business(value TEXT); INSERT INTO shutdown_business VALUES (\'last shutdown write\')');
        writeFileSync(join(root, 'work/shutdown-history'), 'last shutdown write');
        output = { quiesced: true, binding: p.binding };
      } else if (request.operation === 'final-checkpoint-export') {
        expect(stopped).toBe(true);
        const checkpoint = await captureFinalCheckpoint({ binding: p.binding, roots: manifest.ownedRoots, databases: manifest.linux.databases,
          controlDirectory: manifest.controlDirectory, journal: Buffer.from('shutdown journal'), gitRefs: [],
          audit: { manifest, identity: endpoint.instance, state: native.read(), ledger: native.ledger(), managedIpc: { phase: 'stopped', managedIpc: [] } } });
        encrypted = encryptCheckpoint(checkpoint, store.publicKey(a.id));
        output = { checkpointId: checkpoint.checkpointId, manifestDigest: checkpoint.manifestDigest, ciphertextSha256: encrypted.ciphertextSha256, ciphertextBytes: encrypted.ciphertextBytes, uploaded: false };
      } else if (request.operation === 'final-checkpoint-upload') {
        const bytes = openCheckpointCapability(p.capabilityEnvelope, a.binding, workerKey.privateKey);
        const cap = JSON.parse(bytes.toString()); bytes.fill(0);
        const { ciphertextPath, ...descriptor } = encrypted!;
        await custody.receive({ checkpointId: cap.checkpointId, token: cap.token, descriptor, body: createReadStream(ciphertextPath) });
        output = { checkpointId: descriptor.checkpointId, manifestDigest: descriptor.manifestDigest, ciphertextSha256: descriptor.ciphertextSha256, ciphertextBytes: descriptor.ciphertextBytes, uploaded: true };
      } else if (request.operation === 'final-checkpoint-ack') {
        verifyCustodyReceipt(p.receipt, encrypted!, store.publicKey(a.id)); ack = p.receipt; output = seal(p.binding);
      } else if (request.operation === 'seal-status') {
        expect(ack).toBeTruthy(); verifyCustodyReceipt(ack!, encrypted!, store.publicKey(a.id));
        output = { ...p, seal: seal(p.binding), checkpointReceiptDigest: enrollmentDigest(ack) };
      } else throw Error('unexpected root phase');
      const name = args[args.indexOf('--url') + 1]!.split('/runCommands/')[1]!.split('?')[0]!;
      results.set(name, { digest: body.tags.openrigRequest, output: JSON.stringify(output) + '\n' });
      if (unknown) throw Error('uncertain submission');
      return '';
    }
    if (args[1] === 'run-command') {
      if (unknown) throw Error('unknown original command');
      const name = args[args.indexOf('--name') + 1]!, saved = results.get(name)!;
      return JSON.stringify({ id: `${childId}/runCommands/${name}`, name, tags: { openrigRequest: saved.digest }, instanceView: { executionState: rootStatusPending && name.startsWith('openrig-seal-status-') ? 'Running' : 'Succeeded', exitCode: 0, output: saved.output } });
    }
    if (args[0] === 'rest') return JSON.stringify({ value: [] });
    if (args[1] === 'nic') return JSON.stringify([{ id: childId + '/networkInterfaces/nic', virtualMachine: { id: childId }, ipConfigurations: [{ privateIPAddress: '10.0.0.2' }] }]);
    const child = { id: childId, vmId, instanceId: '0', protectionPolicy: { protectFromScaleIn: true, protectFromScaleSetActions: true }, provisioningState: 'Succeeded', storageProfile: { imageReference }, networkProfile: { networkInterfaces: [{ id: childId + '/networkInterfaces/nic' }] } };
    if (args.includes('--instance-id')) { if (pauseBeforePut) db.prepare('UPDATE elastic_fleet_config SET paused=1').run(); return JSON.stringify(child); }
    if (args[1] === 'list-instances') { lists++; if (operations.at(-1) === 'seal-status') finalHook?.(); if (replacement) child.vmId = '22222222-2222-4222-8222-222222222222'; if (slowFinal && operations.at(-1) === 'seal-status') { clock += 5001; mono += 5001; } return JSON.stringify(deleted ? [] : [child]); }
    if (args[1] === 'delete-instances') { operations.push('delete'); deleted = true; if (unknownDelete) throw Error('unknown delete acknowledgment'); return ''; }
    return JSON.stringify({ id: manifest.resourceId, location: 'fixture', tags: { pool: 'fixture' }, orchestrationMode: 'Uniform', provisioningState: 'Succeeded', overprovision: false,
      sku: { capacity: deleted ? 0 : 1 }, upgradePolicy: { mode: 'Manual', automaticOSUpgradePolicy: { enableAutomaticOSUpgrade: false } }, automaticRepairsPolicy: { enabled: false }, virtualMachineProfile: { storageProfile: { imageReference } } });
  };
  const commands = new AzureWorkerCommandClient({ executable: '/fixture/az', azureConfigDirectory: root, privateDirectory: join(root, 'commands'), run });
  const enrollment = new ElasticWorkerEnrollment({ store, commands, manifestRaw, preparationPlan: plan, fleetEpoch: 'epoch', workerBuildCommit: 'c'.repeat(40), artifact,
    packageRecipe: recipe, checkpointCaPem: 'fixture', stageScript: script, helperScript: script, registryPath: registry, now: () => clock,
    fetchImpl: async (url) => {
      if(String(url).endsWith('/api/elastic-worker/continuations')) {
        const outboxId=`wake-intent-${replyId}`;
        const candidates=replyId?[{outboxId,queueDigest:nativeContinuation(worker,outboxId).native.queue_digest,target:continuationTarget}]:[];
        return new Response(JSON.stringify({state:native.read(),candidates}));
      }
      if (stopped) throw Error('worker HTTP stopped');
      return new Response(JSON.stringify({ version: 1, observedAt: clock, selfHostId: 'worker-host', build: { commit: 'c'.repeat(40), dirty: false }, state: native.read(), ledger: native.ledger(),
        filesystem: { complete: true, blockers: [], files: [], refs: [], digest: 'f'.repeat(64) }, process: { observedAt: clock, complete: true, idle: true, processes: 1, uid: manifest.uid, cgroup: '/system.slice/openrig-elastic.service' }, checkpointAcknowledged: false, drainEligible: true, clean: true,
        preparation: { version: 1, observedAt: clock, manifestDigest, operationId: plan.operationId, planDigest: enrollmentDigest(plan), state: 'pending', rung: 'enrolled', rigId: null, seats: [], repositoryCapabilities: [], publicReason: 'fixture' } }));
    } });
  await enrollment.registeredEndpoint(a, configuration);
  const projection = new ElasticFleetProjection(db, () => clock);
  const makeLifecycle = () => new ElasticManagedLifecycle({ db, owner, store, commands, enrollment, custody, projection, manifestRaw, helperScript: script, registryPath: registry, now: () => clock, monotonic: () => mono });
  const lifecycle = makeLifecycle();
  const instance = { resourceId: childId, instanceId: '0', vmId, workerId: a.workerId, protected: true, provisioningState: 'Succeeded' };
  const operation = (type: FleetMutation['type']): FleetMutation => ({ operationId: type === 'beginDrain' ? 'drain-1' : 'remove-1', type, state: 'pending', workerId: a.workerId, count: 0, targetCapacity: type === 'remove' ? 0 : 1, revision: 1,
    snapshot: { observedAt: clock, now: clock, demands: [], providerAvailable: true, workers: [{ id: a.workerId, managed: true, permanent: false, state: 'draining', capabilities: [], slots: 2, active: 0, observedAt: clock, idleSince: 0, clean: true, pendingHumanReturns: false }] }, error: null, createdAt: clock, updatedAt: clock, policy, configuration: fleetConfig });
  const context = (type: FleetMutation['type']) => ({ configuration, instance, operation: operation(type) });
  const turn = <T>(fn: () => Promise<T>) => owner.run('fleet', async () => { expect(owner.claimFleet('fixture-reconcile')).toBe(true); try { return await fn(); } finally { owner.releaseFleet('fixture-reconcile'); } });
  const io = createAzureElasticFleetIO({ run, now: () => clock, lifecycle, observeNative: async () => { throw Error('not used for removal'); },
    beginDrain: c => lifecycle.beginDrain(c), sealForRemoval: c => lifecycle.sealForRemoval(c), retireRegistration: c => lifecycle.retireRegistration(c) });
  return { root, db, worker, native, a, owner, store, lifecycle, makeLifecycle, custody, projection, enrollment, now:()=>clock, io, operations, context, turn, registry, configuration, instance,
    prepareLateHuman:()=>{lateHuman=true;worker.exec(`INSERT INTO rigs(id,name) VALUES('rig','rig'); INSERT INTO nodes(id,rig_id,logical_id,runtime,cwd) VALUES('node','rig','owner','pi','/fixture');
      INSERT INTO sessions(id,node_id,session_name,resume_type,resume_token) VALUES('session','node','worker-a@elastic','pi_session_file','/fixture/session.jsonl');
      INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES('binding','node','worker-a@elastic','%1');
      INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('tenure','node',1,'occupant','fresh');`);},
    answerLateHuman:()=>{const reply=queue.recordHumanResponse({qitemId:question!.qitemId,requestRevision:question!.humanRequestRevision!,actorSession:'human@kernel',replyId:'answer',response:{kind:'custom',text:'Continue existing work'}});replyId=reply.replyQitemId;return reply;},
    unknown: (value: boolean) => unknown = value, pauseBeforePut: () => pauseBeforePut = true, slowFinal: () => slowFinal = true,
    finalHook: (hook: () => void) => finalHook = hook, unknownDelete: () => unknownDelete = true, dispatchDelay: () => dispatchDelay = true, replacement: () => replacement = true,
    pendingStatus: (value: boolean) => rootStatusPending = value, advanceClock: (wall: number, monotonic: number) => { clock += wall; mono += monotonic; }, lists: () => lists };
}
async function drain(f: Awaited<ReturnType<typeof fixture>>) {
  await expect(f.turn(() => f.lifecycle.beginDrain(f.context('beginDrain')))).rejects.toThrow('pending');
  await f.turn(() => f.lifecycle.beginDrain(f.context('beginDrain')));
}
async function prepare(f: Awaited<ReturnType<typeof fixture>>) {
  await drain(f);
  for (let i = 0; i < 8; i++) await expect(f.turn(() => f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow('pending');
}
it('retains original command/key after unknown submit and reconciles without replay', async () => {
  const f = await fixture(); f.unknown(true);
  await expect(f.turn(() => f.lifecycle.beginDrain(f.context('beginDrain')))).rejects.toThrow();
  const key = f.store.publicKey(f.a.id), first = f.lifecycle.journal.read(f.a.id);
  await expect(f.turn(() => f.lifecycle.beginDrain(f.context('beginDrain')))).rejects.toThrow();
  expect(f.operations).toEqual(['begin-drain']); expect(f.store.publicKey(f.a.id)).toBe(key);
  expect(f.lifecycle.journal.read(f.a.id)).toEqual(first);
  f.unknown(false); await f.turn(() => f.lifecycle.beginDrain(f.context('beginDrain')));
  expect(f.operations).toEqual(['begin-drain']); expect(f.lifecycle.journal.read(f.a.id)?.drain?.generation).toBe(1);
});
it('rechecks pause after ARM command preparation and before PUT', async () => {
  const f = await fixture(); f.pauseBeforePut();
  await expect(f.turn(() => f.lifecycle.beginDrain(f.context('beginDrain')))).rejects.toThrow();
  expect(f.operations).toEqual([]); expect(f.native.read()?.phase).toBe('ready');
});
it('stops before final verification on unsupported platforms and retains full uploaded custody', async () => {
  if (process.platform === 'linux') return;
  const f = await fixture(); await drain(f);
  for (let i = 0; i < 6; i++) await expect(f.turn(() => f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow('pending');
  await expect(f.turn(() => f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow();
  expect(f.operations).toEqual(['begin-drain', 'quiesce', 'final-checkpoint-export', 'final-checkpoint-upload']);
  const state = f.lifecycle.journal.read(f.a.id)!;
  expect(f.custody.status(state.exported!.checkpointId).phase).toBe('uploaded');
  expect(state.deleteIntent).toBeNull(); expect(f.native.read()?.phase).toBe('sealed');
});
it.skipIf(process.platform !== 'linux')('verifies original-key full custody, exact fresh deletion, absence and owned registration retirement', async () => {
  const f = await fixture(); await prepare(f);
  await f.turn(() => f.io.remove(f.context('remove').operation));
  expect(f.operations).toEqual(['begin-drain', 'quiesce', 'final-checkpoint-export', 'final-checkpoint-upload', 'final-checkpoint-ack', 'seal-status', 'delete']);
  const state = f.lifecycle.journal.read(f.a.id)!;
  expect(state).toMatchObject({ beginOperationId: 'drain-1', removalOperationId: 'remove-1', binding: { removalOperationId: 'drain-1' }, deleteConfirmed: true, retired: true });
  const round = state.rounds.at(-1)!;
  expect(round.receipt?.seal.operationId).toBe('drain-1');
  const output = f.store.readPrivateInput(f.a.id, `life-output-${round.command.requestDigest}`);
  expect(typeof output).toBe('string'); expect(sha256(output as string)).toBe(round.command.outputDigest);
  expect(JSON.parse(output as string)).toEqual(round.receipt);
  expect(state.deleteIntent?.operationId).toBe('remove-1');
  expect(readFileSync(f.registry, 'utf8')).toContain('permanent'); expect(readFileSync(f.registry, 'utf8')).not.toContain(f.a.registration!.hostId);
  expect(f.store.publicKey(f.a.id)).toBeTruthy(); expect(f.store.readPrivateInput(f.a.id, 'registration-retired')).toBeTruthy();
});
it.skipIf(process.platform !== 'linux')('unknown status is inspected without replay and prior-turn receipt never authorizes deletion', async () => {
  const f = await fixture(); await prepare(f); f.pendingStatus(true);
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations.filter(x => x === 'seal-status')).toHaveLength(1);
  f.pendingStatus(false);
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete');
  await f.turn(() => f.io.remove(f.context('remove').operation));
  expect(f.operations.filter(x => x === 'seal-status')).toHaveLength(2); expect(f.operations.filter(x => x === 'delete')).toHaveLength(1);
});
it.skipIf(process.platform !== 'linux')('slow final ARM read retains the VM without saving delete authority', async () => {
  const f = await fixture(); await prepare(f); f.slowFinal();
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete'); expect(f.lifecycle.journal.read(f.a.id)?.deleteIntent).toBeNull();
});

it('rejects changed begin/remove association across lifecycle restart before new root commands', async () => {
  const f = await fixture(); await drain(f);
  const changedBegin = f.context('beginDrain'); changedBegin.operation.operationId = 'other-drain';
  await expect(f.turn(() => f.makeLifecycle().beginDrain(changedBegin))).rejects.toThrow('generation_conflict');
  await expect(f.turn(() => f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow('pending');
  const changedRemove = f.context('remove'); changedRemove.operation.operationId = 'other-remove';
  const restarted = f.makeLifecycle();
  await expect(f.turn(() => restarted.sealForRemoval(changedRemove))).rejects.toThrow('retained');
  expect(await restarted.inspectNative({ ...changedRemove, instances: [f.instance] })).toBe('unknown');
  await expect(f.turn(() => restarted.retireRegistration(changedRemove))).rejects.toThrow('retained');
  expect(f.operations).toEqual(['begin-drain', 'quiesce']);
  expect(readFileSync(f.registry, 'utf8')).toContain(f.a.registration!.hostId);
});
it.skipIf(process.platform !== 'linux')('retains a fresh sealed VM when pause races the final ARM read', async () => {
  const f = await fixture(); await prepare(f);
  f.finalHook(() => { f.db.prepare('UPDATE elastic_fleet_config SET paused=1').run(); });
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete'); expect(f.lifecycle.journal.read(f.a.id)?.deleteIntent).toBeNull();
});
it.skipIf(process.platform !== 'linux')('retains a fresh sealed VM after monotonic expiry despite a stationary wall clock', async () => {
  const f = await fixture(); await prepare(f); f.finalHook(() => f.advanceClock(0, 5001));
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete'); expect(f.lifecycle.journal.read(f.a.id)?.deleteIntent).toBeNull();
});
it.skipIf(process.platform !== 'linux')('reconciles unknown delete by exact absence, preserving original intent and refusing changed removal', async () => {
  const f = await fixture(); await prepare(f); f.unknownDelete();
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.lifecycle.journal.read(f.a.id)?.retired).toBe(false);
  const changed = f.context('remove'); changed.operation.operationId = 'other-remove';
  await expect(f.turn(() => f.makeLifecycle().retireAbsent(changed.operation, f.configuration, []))).rejects.toThrow('unknown');
  expect(readFileSync(f.registry, 'utf8')).toContain(f.a.registration!.hostId);
  expect(await f.turn(() => f.io.advanceOperation!(f.context('remove').operation))).toBe('succeeded');
  expect(f.operations.filter(x => x === 'delete')).toHaveLength(1);
  expect(f.lifecycle.journal.read(f.a.id)).toMatchObject({ deleteConfirmed: true, retired: true });
});

it.skipIf(process.platform !== 'linux')('rechecks freshness inside the argv runner after dispatch preparation', async () => {
  const f = await fixture(); await prepare(f); f.dispatchDelay();
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete');
  expect(f.lifecycle.journal.read(f.a.id)?.deleteIntent).not.toBeNull();
  expect(readFileSync(f.registry, 'utf8')).toContain(f.a.registration!.hostId);
});

it.skipIf(process.platform !== 'linux')('refuses a replacement incarnation observed by the final ARM read', async () => {
  const f = await fixture(); await prepare(f); f.finalHook(() => f.replacement());
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete'); expect(f.lifecycle.journal.read(f.a.id)?.deleteIntent).toBeNull();
});
it.skipIf(process.platform !== 'linux')('refuses loss of the exact fleet claim after fresh root status', async () => {
  const f = await fixture(); await prepare(f);
  f.finalHook(() => { f.db.prepare("UPDATE elastic_native_owners SET owner='other-claim' WHERE scope='fleet'").run(); });
  await expect(f.turn(() => f.io.remove(f.context('remove').operation))).rejects.toThrow();
  expect(f.operations).not.toContain('delete'); expect(f.lifecycle.journal.read(f.a.id)?.deleteIntent).toBeNull();
});

it('withdraws an actual native human wake with immutable root begin/remove IDs and never replays unknown withdrawal',async()=>{
  const f=await fixture();f.prepareLateHuman();await drain(f);f.answerLateHuman();
  expect(await f.turn(()=>f.lifecycle.hasOwnedContinuation(f.a.workerId,f.context('remove').operation.configuration))).toBe(true);
  f.unknown(true);await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow();
  const first=f.lifecycle.journal.read(f.a.id)!;
  expect(first).toMatchObject({beginOperationId:'drain-1',removalOperationId:'remove-1',binding:{removalOperationId:'drain-1'},withdrawal:{receipt:null}});
  expect(f.native.read()).toMatchObject({phase:'draining',generation:2});
  await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow();
  expect(f.operations).toEqual(['begin-drain','withdraw-drain']);
  f.unknown(false);await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow('withdrawn');
  expect(await f.lifecycle.inspectNative({...f.context('remove'),operation:f.context('remove').operation})).toBe('withdrawn');
  const state=f.lifecycle.journal.read(f.a.id)!;expect(state.withdrawal?.receipt?.generation).toBe(2);expect(state.deleteIntent).toBeNull();
  expect(f.operations).toEqual(['begin-drain','withdraw-drain']);
  const oldInput=first.commands.find(c=>c.operation==='begin-drain')!;
  const bytes=f.store.readPrivateInput(f.a.id,oldInput.inputName);
  await f.turn(async()=>{const finished=f.lifecycle.journal.advance(state,{withdrawal:{...state.withdrawal!,finished:true}});expect(finished.withdrawal?.finished).toBe(true);
    const next=f.lifecycle.journal.begin(f.a.id,'drain-3');expect(next.commands).toEqual([]);expect(next.beginOperationId).toBe('drain-3');
    expect(()=>f.lifecycle.journal.begin(f.a.id,'drain-1')).toThrow();});
  expect(f.store.readPrivateInput(f.a.id,oldInput.inputName)).toEqual(bytes);
});
it.each(['intent','submitted','unknown'] as const)('never withdraws after a persisted quiesce %s',async state=>{
  const f=await fixture();await drain(f);
  await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow();
  const current=f.lifecycle.journal.read(f.a.id)!;
  await f.turn(async()=>{f.lifecycle.journal.advance(current,{commands:current.commands.map(c=>c.operation==='quiesce'?{...c,state}:c)});});
  f.unknown(true);await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow();
  expect(f.operations).toEqual(['begin-drain','quiesce']);expect(f.lifecycle.journal.read(f.a.id)?.withdrawal).toBeNull();
});

it.each(['projection','idle-delete'] as const)('repairs a finished journal after a real SQLite %s failure exactly once, then drains in a new generation',async point=>{
  const f=await fixture();f.prepareLateHuman();await drain(f);f.answerLateHuman();
  await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow();
  await expect(f.turn(()=>f.lifecycle.sealForRemoval(f.context('remove')))).rejects.toThrow('withdrawn');
  const operation=f.context('remove').operation,config=operation.configuration;
  f.db.prepare("INSERT INTO elastic_fleet_operations VALUES(?,?,'withdrawn',?,?,?,?,?,?,NULL,?,?)").run(operation.operationId,operation.type,operation.workerId,operation.count,operation.targetCapacity,operation.revision,JSON.stringify(operation.snapshot),JSON.stringify(config),operation.createdAt,operation.updatedAt);
  f.db.prepare("INSERT INTO elastic_fleet_drains VALUES(?,?,'continuation','drain-1',?,?)").run(f.a.workerId,config.policy.poolId,f.now(),f.now());
  f.db.prepare('INSERT INTO elastic_idle_windows VALUES(?,?,?,?,?,?)').run(f.a.workerId,'old','old','old',0,0);
  // Native runtime consumption is covered in the runner fixture; seed only that proof for this coordinator crash test.
  f.worker.exec("UPDATE elastic_continuation_actions SET stage='proved',proved_at=1; UPDATE elastic_worker_generations SET progress_at=1 WHERE generation=2; UPDATE queue_items SET state='done',ts_updated='fresh-disposition'; UPDATE outbox_entries SET delivery_state='delivered'");
  f.db.exec(point==='projection'?"CREATE TRIGGER fail_finish BEFORE UPDATE ON elastic_fleet_drains WHEN NEW.state='continuation-finished' BEGIN SELECT RAISE(ABORT,'crash_after_finished_journal'); END":"CREATE TRIGGER fail_finish BEFORE DELETE ON elastic_idle_windows BEGIN SELECT RAISE(ABORT,'crash_during_idle_reset'); END");
  await f.turn(()=>f.lifecycle.reconcileContinuations(config));
  await f.turn(()=>f.lifecycle.reconcileContinuations(config));
  const finished=f.lifecycle.journal.read(f.a.id)!;
  expect(finished.withdrawal?.finished).toBe(true);
  expect(finished.commands.at(-1)).toMatchObject({operation:'finish-continuation',state:'succeeded'});
  expect(f.db.prepare('SELECT state FROM elastic_fleet_drains').get()).toEqual({state:'continuation'});
  expect(f.db.prepare('SELECT first_at FROM elastic_idle_windows').get()).toEqual({first_at:0});
  f.db.exec('DROP TRIGGER fail_finish');
  const restarted=f.makeLifecycle();
  await f.turn(()=>restarted.reconcileContinuations(config));
  expect(f.db.prepare('SELECT state FROM elastic_fleet_drains').get()).toEqual({state:'continuation-finished'});
  expect(f.db.prepare('SELECT * FROM elastic_idle_windows').get()).toBeUndefined();
  expect(restarted.journal.read(f.a.id)).toEqual(finished);
  const observer=new ElasticFleetProjection(f.db,f.now);
  const calls: FleetMutation[]=[];
  const service=new ElasticFleetService(f.db,{
    observe:async()=>{
      await restarted.reconcileContinuations(config);
      const observed=await f.enrollment.observeManagedWorker(f.a,f.configuration);
      const worker={...operation.snapshot.workers[0]!,observedAt:f.now()};
      const idleSince=observer.idleSince(f.a,{...observed,worker},120000);
      return {...operation.snapshot,now:f.now(),observedAt:f.now(),workers:[{...worker,idleSince}]};
    },
    beginDrain:async op=>{calls.push(op);await restarted.beginDrain({...f.context('beginDrain'),operation:op});},
    provision:async()=>{throw Error('unexpected provision');},remove:async()=>{throw Error('unexpected removal');},
    inspectOperation:async op=>{await restarted.beginDrain({...f.context('beginDrain'),operation:{...op,configuration:config,policy:config.policy}});return 'succeeded';},
  },f.now,f.owner);
  await service.reconcile();const since=f.now();
  expect(f.db.prepare('SELECT first_at FROM elastic_idle_windows').get()).toEqual({first_at:since});
  // A later drain row and its new idle interval cannot be reset by the older finished journal.
  f.db.prepare("UPDATE elastic_fleet_drains SET begin_operation_id='later-drain',state='continuation'").run();
  await f.turn(()=>restarted.reconcileContinuations(config));
  expect(f.db.prepare('SELECT state,begin_operation_id FROM elastic_fleet_drains').get()).toEqual({state:'continuation',begin_operation_id:'later-drain'});
  expect(f.db.prepare('SELECT first_at FROM elastic_idle_windows').get()).toEqual({first_at:since});
  f.db.prepare("UPDATE elastic_fleet_drains SET begin_operation_id='drain-1',state='continuation-finished'").run();
  for(let i=0;i<14;i++){f.advanceClock(60000,60000);await service.reconcile();}
  f.advanceClock(59999,59999);await service.reconcile();expect(calls).toEqual([]);
  expect(f.db.prepare('SELECT first_at FROM elastic_idle_windows').get()).toEqual({first_at:since});
  f.advanceClock(1,1);await service.reconcile();expect(calls).toHaveLength(1);
  expect(calls[0]!.operationId).not.toBe('drain-1');
  await service.reconcile();
  expect(f.native.read()).toMatchObject({generation:3,drainOperationId:calls[0]!.operationId});
  expect(restarted.journal.read(f.a.id)).toMatchObject({beginOperationId:calls[0]!.operationId,drain:{generation:3}});
  expect(f.operations.filter(op=>op==='finish-continuation')).toHaveLength(1);
});
