import type Database from 'better-sqlite3';
import { execFile } from 'node:child_process';
import { lstatSync, realpathSync, openSync, closeSync, readFileSync, fstatSync, constants } from 'node:fs';
import { createHash } from 'node:crypto';
import { z } from 'zod';
import type { SessionRegistry } from '../domain/session-registry.js';
import type { QueueRepository } from '../domain/queue-repository.js';
import { ElasticFleetService, type FleetIO, type FleetMutation } from '../domain/elastic-fleet.js';
import { ElasticFleetProjection } from '../domain/elastic-fleet-projection.js';
import { GithubIntakeService, type GithubIntakeIO } from '../domain/github-intake.js';
import { PollScheduler } from '../domain/github-poll-scheduler.js';
import type { FleetSnapshot, FleetWorkerObservation } from '../domain/elastic-fleet-policy.js';
import { loadElasticDeploymentProfile } from './elastic-deployment-profile.js';
import { ElasticNativeOwner } from './elastic-native-owner.js';
import { ElasticWorkerAttemptStore, enrollmentDigest } from './elastic-worker-attempts.js';
import { AzureWorkerCommandClient } from './azure-worker-command.js';
import { createAzureElasticFleetIO, validateAzurePoolConfiguration, type AzureArgvRunner } from './azure-elastic-fleet.js';
import { ElasticWorkerEnrollment } from './elastic-worker-enrollment.js';
import { bindCoordinatorReadinessPlan, ElasticEnrolledWorkerDirectory } from './elastic-coordinator-admission.js';
import { ElasticHumanForwarder } from './elastic-human-forwarder.js';
import { CheckpointCustody } from './elastic-checkpoint-custody.js';
import { ElasticManagedLifecycle } from './elastic-managed-lifecycle.js';
import { preflightArchivedSqlite } from './elastic-checkpoint-sqlite-isolation.js';
import { openRuntimeArtifact, type RuntimeArtifact } from './elastic-runtime-artifact.js';
import { listenPrivateCheckpoints } from './elastic-checkpoint-receiver.js';
import { isHumanSeatSessionRef } from '../domain/session-name.js';

export interface ElasticRuntimeOptions {
  db: Database.Database; databasePath: string; sessions: SessionRegistry; hostId: string;
  profilePath?: string; report?: (message: string) => void;
}
/** No profile means no private files, native owner, subprocess, listener or network activity. */
export function createElasticRuntime(options: ElasticRuntimeOptions): ElasticRuntime | undefined {
  return options.profilePath ? new ElasticRuntime({ ...options, profilePath: options.profilePath }) : undefined;
}
export class ElasticRuntime {
  readonly owner: ElasticNativeOwner;
  readonly store: ElasticWorkerAttemptStore;
  readonly projection: ElasticFleetProjection;
  readonly directory: ElasticEnrolledWorkerDirectory;
  readonly service: ElasticFleetService;
  private readonly loaded;
  private readonly commands: AzureWorkerCommandClient;
  private readonly enrollment: ElasticWorkerEnrollment;
  private readonly custody: CheckpointCustody;
  private artifact?: RuntimeArtifact;
  private listener?: Awaited<ReturnType<typeof listenPrivateCheckpoints>>;
  private activating?: Promise<void>;
  private intake?: GithubIntakeService;
  private baseIO?: GithubIntakeIO;
  private human?: PollScheduler;
  private fleet: PollScheduler;
  private closed = false;
  private snapshot: FleetSnapshot = { observedAt: 0, now: 0, demands: [], workers: [], providerAvailable: false };
  constructor(private readonly options: ElasticRuntimeOptions & { profilePath: string }) {
    this.loaded = loadElasticDeploymentProfile(options.profilePath);
    const { profile, preparationPlan } = this.loaded;
    if (!isHumanSeatSessionRef(profile.coordinatorHuman) || !isHumanSeatSessionRef(profile.originHuman)) throw Error('elastic_human_binding_required');
    const plan = bindCoordinatorReadinessPlan({ plan: preparationPlan, sessions: options.sessions, manager: profile.coordinatorManager, hostId: options.hostId });
    this.owner = new ElasticNativeOwner(options.db, options.databasePath);
    this.store = new ElasticWorkerAttemptStore(profile.attemptDirectory, this.owner);
    this.projection = new ElasticFleetProjection(options.db);
    const run: AzureArgvRunner = (args, limits) => new Promise((resolve, reject) => {
      this.assertAzureEntry();
      limits.beforeDispatch?.();
      execFile(profile.azure.executable, [...args], { encoding: 'utf8', timeout: limits.timeout, maxBuffer: limits.maxBuffer, shell: false,
        env: { PATH: '/usr/bin:/bin', HOME: profile.azure.configDirectory, AZURE_CONFIG_DIR: profile.azure.configDirectory,
          AZURE_CORE_COLLECT_TELEMETRY: '0', AZURE_EXTENSION_USE_DYNAMIC_INSTALL: 'no' } },
      (error, stdout) => error ? reject(Error('elastic_azure_command_unknown')) : resolve(stdout));
    });
    this.commands = new AzureWorkerCommandClient({ executable: profile.azure.executable, azureConfigDirectory: profile.azure.configDirectory, privateDirectory: profile.commandDirectory, run });
    this.enrollment = new ElasticWorkerEnrollment({ store: this.store, commands: this.commands, manifestRaw: this.loaded.manifestRaw, preparationPlan: plan,
      fleetEpoch: profile.fleetEpoch, workerBuildCommit: profile.workerBuildCommit, artifact: this.loaded.artifact, packageRecipe: this.loaded.packageRecipe,
      assertMutation: context => this.assertMutation(context.operation), checkpointCaPem: this.loaded.caPem, stageScript: profile.stageScript, helperScript: profile.helperScript, registryPath: profile.registryPath });
    const configuration = () => {
      const current = this.service.status().config;
      if (!current) throw Error('elastic_not_configured');
      return validateAzurePoolConfiguration(current.provider, current.policy.poolId);
    };
    this.directory = new ElasticEnrolledWorkerDirectory({ store: this.store, enrollment: this.enrollment, configuration, sessions: options.sessions,
      coordinatorManager: profile.coordinatorManager, coordinatorHuman: profile.coordinatorHuman, originHuman: profile.originHuman, projection: this.projection });
    this.custody = new CheckpointCustody(profile.custody);
    const lifecycle = new ElasticManagedLifecycle({ db: options.db, owner: this.owner, store: this.store, commands: this.commands, enrollment: this.enrollment,
      custody: this.custody, projection: this.projection, manifestRaw: this.loaded.manifestRaw, helperScript: profile.helperScript, registryPath: profile.registryPath });
    const provider = createAzureElasticFleetIO({ run, lifecycle, assertMutation: operation => this.assertMutation(operation),
      observeNative: async ({ instances }) => {
        const state = this.service.status(), cfg = this.intake?.status().config;
        if (!cfg || !this.baseIO) throw Error('elastic_intake_not_composed');
        const observations = await this.baseIO.observeTargets(cfg);
        const managed = await this.directory.refresh((state.config?.policy.pollIntervalSeconds ?? 60) * 1000 + 5000);
        const at = Date.now();
        const permanentWorkers: FleetWorkerObservation[] = [...new Set(cfg.targets.map(t => t.physicalHost))].map(id => {
          const targets = cfg.targets.filter(t => t.physicalHost === id);
          const sampled = targets.map(t => observations.find(o => o.host === t.host && o.session === t.session));
          const complete = sampled.every(o => o?.available && o.hostActive !== null);
          return { id, permanent: true, managed: false, state: complete ? 'ready' : 'quarantined',
            capabilities: [...new Set(targets.flatMap(t => t.capabilities))], slots: targets.reduce((n, t) => n + t.capacity, 0),
            active: complete ? sampled.reduce((n, o) => n + Math.min(1, o!.active), 0) : null,
            observedAt: at, idleSince: null, clean: complete, pendingHumanReturns: complete ? false : null };
        });
        const rows = options.db.prepare("SELECT repository_id,issue_number,capability FROM github_intake_issues WHERE in_scope=1 AND eligibility='ready' AND approved_revision=json_extract(issue_json,'$.updatedAt') AND qitem_id IS NULL").all();
        const demands = z.array(z.object({ repository_id: z.string(), issue_number: z.number().int(), capability: z.string() })).parse(rows)
          .map(r => ({ id: `${r.repository_id}:${r.issue_number}`, capability: r.capability }));
        const attempts = this.store.list();
        return { observedAt: at, demands, permanentWorkers,
          managedWorkers: instances.map(i => managed.find(w => w.id === i.workerId) ?? { id: i.workerId, managed: true, permanent: false, state: 'quarantined', capabilities: [], slots: null, active: null, observedAt: at, idleSince: null, clean: null, pendingHumanReturns: null }),
          provisioningAttempts: attempts.filter(a => instances.some(i => i.vmId === a.binding.vmId)).map(a => ({ workerId: a.workerId, vmId: a.binding.vmId, operationId: a.identity.operationId })) };
      },
      bootstrap: async context => {
        this.assertMutation(context.operation);
        const result = await this.enrollment.advanceEnrollment(context);
        const attempt = this.store.list().find(a => a.workerId === context.instance.workerId && a.identity.operationId === context.operation.operationId);
        if (attempt?.registration) { this.assertMutation(context.operation); await this.directory.readiness(attempt.id); }
        if (result.state !== 'ready') throw Error(result.publicReason);
      },
      beginDrain: context => lifecycle.beginDrain(context), sealForRemoval: context => lifecycle.sealForRemoval(context),
      retireRegistration: context => lifecycle.retireRegistration(context),
      inspectNative: context => context.operation.type === 'provision' ? this.enrollment.inspectNative(context) : lifecycle.inspectNative(context),
    });
    const io: FleetIO = {
      recoverAttempts: () => this.store.recoverStoppedLocks(),
      validateConfiguration: config => {
        provider.validateConfiguration?.(config);
        const azure = validateAzurePoolConfiguration(config.provider, config.policy.poolId);
        const image = azure.image.kind === 'id' ? azure.image.id : `${azure.image.publisher}:${azure.image.offer}:${azure.image.sku}:${azure.image.version}`;
        if (azure.deploymentProfileRef !== options.profilePath || azure.resourceId !== this.loaded.manifest.resourceId
          || azure.bootstrap.digest !== plan.manifestDigest || image !== this.loaded.manifest.linux.imageReference
          || config.policy.slotsPerWorker !== plan.seats.length) throw Error('elastic_profile_binding_conflict');
      },
      observe: async (policy, config, purpose) => {
        // Explicit purpose prevents an overlapping plan request borrowing a held reconcile scope.
        if (purpose !== 'reconcile') return { ...this.snapshot, now: Date.now() };
        try { this.owner.assertHeld('fleet'); } catch { return { ...this.snapshot, now: Date.now() }; }
        if (!policy.enabled || policy.mode !== 'active' || this.service.status().paused) return { ...this.snapshot, now: Date.now() };
        await this.activate();
        this.snapshot = await provider.observe(policy, config);
        return this.snapshot;
      },
      inspectOperation: async (operation, config) => {
        this.owner.assertHeld('fleet');
        // Existing active effects may be read while paused. Disabled/dry-run do no ARM work.
        const current = this.service.status().config;
        if (!current?.policy.enabled || current.policy.mode !== 'active') return 'unknown';
        return provider.inspectOperation(operation, config);
      },
      advanceOperation: async operation => { this.assertMutation(operation); await this.activate(); return provider.advanceOperation!(operation); },
      provision: async operation => { this.assertMutation(operation); await this.activate(); return provider.provision(operation); },
      beginDrain: async operation => { this.assertMutation(operation); await this.activate(); return provider.beginDrain(operation); },
      remove: async operation => { this.assertMutation(operation); await this.activate(); return provider.remove(operation); },
    };
    this.service = new ElasticFleetService(options.db, io, Date.now, this.owner);
    this.fleet = new PollScheduler({ status: () => ({ config: { pollIntervalSeconds: this.service.status().config?.policy.pollIntervalSeconds ?? 60 } }),
      reconcile: () => this.service.reconcile() }, options.report);
    const existing = this.service.status().config;
    if (existing) io.validateConfiguration!(existing);
  }
  private assertMutation(operation: FleetMutation): void {
    this.owner.assertHeld('fleet');
    const state = this.service.status();
    if (this.closed || state.paused || !state.config?.policy.enabled || state.config.policy.mode !== 'active'
      || enrollmentDigest(state.config) !== enrollmentDigest(operation.configuration)) throw Error('elastic_mutation_not_authorized');
  }
  private assertAzureEntry(): void {
    const { profile } = this.loaded;
    const executable = lstatSync(profile.azure.executable), config = lstatSync(profile.azure.configDirectory);
    if (realpathSync(profile.azure.executable) !== profile.azure.executable || !executable.isFile() || executable.nlink !== 1 || !(executable.mode & 0o111)
      || executable.mode & 0o022 || (executable.uid !== 0 && executable.uid !== process.getuid?.())
      || realpathSync(profile.azure.configDirectory) !== profile.azure.configDirectory || !config.isDirectory() || config.uid !== process.getuid?.() || config.mode & 0o077)
      throw Error('elastic_azure_installation_rejected');
    const fd = openSync(profile.azure.executable, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const actual = fstatSync(fd);
      if (actual.ino !== executable.ino || actual.dev !== executable.dev || actual.size > 64 * 1024 ** 2
        || createHash('sha256').update(readFileSync(fd)).digest('hex') !== profile.azure.sha256) throw Error('elastic_azure_executable_changed');
    } finally { closeSync(fd); }
  }
  private async activate(): Promise<void> {
    if (this.closed) throw Error('elastic_runtime_closed');
    this.owner.assertHeld('fleet');
    if (this.listener) return;
    if (this.activating) return this.activating;
    this.activating = (async () => {
      const { profile } = this.loaded;
      this.assertAzureEntry();
      preflightArchivedSqlite(profile.custody.directory);
      this.artifact = openRuntimeArtifact(profile.artifact);
      try {
        const cfg = this.service.status().config;
        if (!cfg?.policy.enabled || cfg.policy.mode !== 'active' || this.service.status().paused) throw Error('elastic_mutation_not_authorized');
        const azure = validateAzurePoolConfiguration(cfg.provider, cfg.policy.poolId);
        const account = z.object({ id: z.string(), state: z.literal('Enabled') }).passthrough().parse(await this.commands.run(['account', 'show', '--subscription', azure.subscription]));
        if (account.id.toLowerCase() !== azure.subscription.toLowerCase()) throw Error('elastic_azure_account_changed');
        if (this.closed || this.service.status().paused || enrollmentDigest(this.service.status().config) !== enrollmentDigest(cfg)) throw Error('elastic_mutation_not_authorized');
        this.listener = await listenPrivateCheckpoints(this.custody, this.loaded.listener, this.artifact);
      } catch (error) { this.artifact.close(); this.artifact = undefined; throw error; }
    })();
    try { await this.activating; } finally { this.activating = undefined; }
  }
  /** Attach the application's only queue after constructing its live authority closure. */
  attach(queue: QueueRepository, baseIO: GithubIntakeIO): GithubIntakeService {
    if (this.intake) throw Error('elastic_runtime_already_attached');
    this.baseIO = baseIO;
    const wrapped = this.directory.intakeIO(baseIO);
    this.intake = new GithubIntakeService(this.options.db, queue, { ...wrapped, observeTargets: async config => {
      const state = this.service.status();
      const result = await wrapped.observeTargets(config);
      if (state.config?.policy.enabled && state.config.policy.mode === 'active' && !state.paused) {
        await this.directory.refresh(state.config.policy.pollIntervalSeconds * 1000 + 5000);
      }
      return result;
    } }, () => new Date(), this.projection);
    const forwarder = new ElasticHumanForwarder({ db: this.options.db, queue, owner: this.owner, coordinatorHostId: this.options.hostId,
      coordinatorManager: this.loaded.profile.coordinatorManager, coordinatorHuman: this.loaded.profile.coordinatorHuman, directory: this.directory });
    this.human = new PollScheduler({ status: () => ({ config: { pollIntervalSeconds: 5 } }), reconcile: async () => {
      const config = this.service.status().config;
      if (config?.policy.enabled && config.policy.mode === 'active' && this.store.list().some(a => a.registration)) await forwarder.run();
      return { error: null };
    } }, this.options.report);
    return this.intake;
  }
  start(): void { if (this.closed || !this.intake) throw Error('elastic_runtime_unavailable'); this.fleet.start(); this.human?.start(); }
  async close(): Promise<void> {
    this.closed = true;
    await Promise.all([this.fleet.stop(), this.human?.stop()]);
    try { await this.activating; } finally { this.artifact?.close(); await this.listener?.close(); }
  }
}
