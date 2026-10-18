import { createHash, createPrivateKey, createPublicKey, generateKeyPairSync, randomBytes } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';
import { openWorkerMaterial, sealEnrollment, type EnrollmentBinding } from '../domain/elastic-enrollment.js';
import { ElasticWorkerControl, type ElasticWorkerIdentity, workerDigest } from '../domain/elastic-worker-control.js';
import { inspectElasticWorker, type WorkerCheckpoint, type WorkerProcessProof } from './elastic-worker-probe.js';
import { azureWorkerId, type AzureRemovalSeal } from './azure-elastic-fleet.js';

export interface WorkerBootstrapManifest {
  version: 1; resourceId: string; workerUser: 'openrig-elastic'; unit: 'openrig-elastic.service'; uid: number; gid: number;
  controlDirectory: string; ownedRoots: string[]; bindPolicy: 'private-ipv4'; port: number;
  /** Root helper, Node, OpenRig/Pi entrypoints AND complete installed source/dependency trees. */
  pins: Array<{ path: string; sha256: string; kind: 'file' | 'tree' }>;
  /** Controller-local private file pointers; inert on the worker, never file values. */
  materialFiles: { piAuthFile: string; piSettingsFile: string; piModelsFile: string };
}
export interface WorkerRuntimeMaterial { version: 1; piAuth: Record<string, unknown>; piModels: Record<string, unknown>; piSettings: Record<string, unknown> }
/**
 * Privileged implementation must run OUTSIDE the managed unit. No default implementation:
 * enabling cloud bootstrap requires the independently verified Linux/image boundary.
 */
export interface PrivilegedWorkerBoundary {
  /** Verify euid=0/Linux, root-owned manifest/pins/ancestors, dedicated UID, no alternate launch authority. */
  assertTrustedEnvironment(manifest: WorkerBootstrapManifest, manifestDigest: string): void;
  /** Install only pinned programs/config; root-owned unit, User=openrig-elastic, KillMode=control-group. */
  configureWorker(material: WorkerRuntimeMaterial, bearer: string): void;
  startUnit(): void;
  /** Durable root-owned ExecCondition marker; workers cannot remove it, and restart/boot must respect it. */
  installSealMarker(): void;
  stopUnit(): void;
  /** Fresh UID+cgroup census and independently established idle/tool evidence, never daemon clean booleans. */
  processes(): WorkerProcessProof;
}
interface PreparedWorker { binding: EnrollmentBinding; controllerPublicKeyPem: string; privateKeyPem: string; publicKeyPem: string; bearer: string; enrolled: boolean; materialDigest: string | null }
function reject(): never { throw Error('worker_bootstrap_rejected'); }
function object(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function atomicPrivateJson(path: string, value: unknown): void {
  const temporary = `${path}.${randomBytes(8).toString('hex')}.tmp`; let fd: number | undefined;
  try { fd = openSync(temporary, 'wx', 0o600); writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); closeSync(fd); fd = undefined; renameSync(temporary, path); }
  finally { if (fd !== undefined) closeSync(fd); rmSync(temporary, { force: true }); }
}
/** Do not pass extension provisioningState here: only managed Run Command instanceView script fields. */
export function assertManagedCommandSuccess(value: unknown): string {
  if (!object(value) || value.executionState !== 'Succeeded' || value.exitCode !== 0 || typeof value.output !== 'string'
    || Buffer.byteLength(value.output) > 4096 || value.output.length === 0) reject();
  return value.output;
}
/** Controller-only: read explicit local files, encrypt before handing any input to Azure. */
export function readWorkerRuntimeMaterial(refs: { piAuthFile: string; piSettingsFile: string; piModelsFile: string }): Buffer {
  try {
    if (!isAbsolute(refs.piAuthFile) || !isAbsolute(refs.piSettingsFile) || !isAbsolute(refs.piModelsFile)) reject();
    const auth = readFileSync(refs.piAuthFile); const settings = readFileSync(refs.piSettingsFile); const models = readFileSync(refs.piModelsFile);
    if (auth.length + settings.length + models.length > 60 * 1024) reject();
    const piAuth: unknown = JSON.parse(auth.toString()); const piSettings: unknown = JSON.parse(settings.toString()); const piModels: unknown = JSON.parse(models.toString());
    if (!object(piAuth) || !object(piSettings) || !object(piModels)) reject();
    return Buffer.from(JSON.stringify({ version: 1, piAuth, piSettings, piModels }));
  } catch { return reject(); }
}
export class AzureWorkerBootstrap {
  constructor(readonly manifest: WorkerBootstrapManifest, readonly manifestDigest: string,
    readonly control: ElasticWorkerControl, private readonly os: PrivilegedWorkerBoundary) {}
  private guarded<T>(operation: () => T): T {
    let lock: string | undefined;
    try {
      this.os.assertTrustedEnvironment(this.manifest, this.manifestDigest);
      if (this.manifest.version !== 1 || this.manifest.workerUser !== 'openrig-elastic' || this.manifest.unit !== 'openrig-elastic.service'
        || !isAbsolute(this.manifest.controlDirectory) || this.manifest.uid <= 0 || !Number.isSafeInteger(this.manifest.uid)
        || this.manifest.gid <= 0 || !Number.isSafeInteger(this.manifest.gid) || !/^[a-f0-9]{64}$/.test(this.manifestDigest)) reject();
      mkdirSync(this.manifest.controlDirectory, { recursive: true, mode: 0o700 });
      const candidate = join(this.manifest.controlDirectory, 'operation.lock');
      const fd = openSync(candidate, 'wx', 0o600); closeSync(fd); lock = candidate;
      return operation();
    } catch { return reject(); }
    finally { if (lock) rmSync(lock, { force: true }); }
  }
  private path(name: string): string { return join(this.manifest.controlDirectory, name); }
  private persist(name: string, value: unknown): void {
    atomicPrivateJson(this.path(name), value);
    const fd = openSync(this.manifest.controlDirectory, 'r'); try { fsyncSync(fd); } finally { closeSync(fd); }
  }
  private identity(binding: EnrollmentBinding): ElasticWorkerIdentity {
    if (binding.manifestDigest !== this.manifestDigest || binding.resourceId !== this.manifest.resourceId) reject();
    return { workerId: azureWorkerId(this.manifest.resourceId, binding.vmId), vmId: binding.vmId, manifestDigest: binding.manifestDigest };
  }
  private requireState(identity: ElasticWorkerIdentity, generation?: number) {
    const state = this.control.read();
    if (!state || identity.workerId !== azureWorkerId(this.manifest.resourceId, identity.vmId) || identity.manifestDigest !== this.manifestDigest
      || state.workerId !== identity.workerId || state.vmId !== identity.vmId || state.manifestDigest !== identity.manifestDigest
      || (generation !== undefined && state.generation !== generation)) reject();
    return state;
  }
  private prepared(binding: EnrollmentBinding): PreparedWorker {
    const prepared = JSON.parse(readFileSync(this.path('enrollment.json'), 'utf8')) as PreparedWorker;
    if (workerDigest(prepared.binding) !== workerDigest(binding)) reject(); this.identity(binding); return prepared;
  }
  prepare(binding: EnrollmentBinding, controllerPublicKeyPem: string): { version: 1; binding: EnrollmentBinding; publicKeyPem: string } {
    return this.guarded(() => {
      this.identity(binding);
      // The established output codec validates every binding field and controller RSA modulus.
      sealEnrollment(Buffer.from('validation'), binding, createPublicKey(controllerPublicKeyPem));
      let prepared: PreparedWorker;
      if (existsSync(this.path('enrollment.json'))) {
        prepared = this.prepared(binding); if (prepared.controllerPublicKeyPem !== controllerPublicKeyPem) reject();
      } else {
        const key = generateKeyPairSync('rsa', { modulusLength: 3072 });
        prepared = { binding, controllerPublicKeyPem,
          privateKeyPem: key.privateKey.export({ format: 'pem', type: 'pkcs8' }).toString(),
          publicKeyPem: key.publicKey.export({ format: 'pem', type: 'spki' }).toString(),
          bearer: randomBytes(48).toString('base64url'), enrolled: false, materialDigest: null };
        this.persist('enrollment.json', prepared);
      }
      const result = { version: 1 as const, binding, publicKeyPem: prepared.publicKeyPem };
      if (Buffer.byteLength(JSON.stringify(result)) > 4096) reject(); return result;
    });
  }
  enroll(binding: EnrollmentBinding, materialEnvelope: string): string {
    return this.guarded(() => {
      const prepared = this.prepared(binding); const identity = this.identity(binding);
      const plaintext = openWorkerMaterial(materialEnvelope, binding, createPrivateKey(prepared.privateKeyPem));
      try {
        const material: unknown = JSON.parse(plaintext.toString());
        if (!object(material) || Object.keys(material).sort().join(',') !== 'piAuth,piModels,piSettings,version'
          || material.version !== 1 || !object(material.piAuth) || !object(material.piSettings) || !object(material.piModels)) reject();
        const digest = createHash('sha256').update(plaintext).digest('hex');
        if (prepared.materialDigest && prepared.materialDigest !== digest) reject();
        if (!prepared.enrolled) {
          // Persist input identity before side effects: a lost reply resumes this exact bearer/config.
          prepared.materialDigest = digest; this.persist('enrollment.json', prepared);
          this.os.configureWorker(material as unknown as WorkerRuntimeMaterial, prepared.bearer);
          this.control.enroll(identity); this.os.startUnit(); prepared.enrolled = true; this.persist('enrollment.json', prepared);
        } else if (this.requireState(identity).phase !== 'ready') reject();
        return sealEnrollment(Buffer.from(prepared.bearer), binding, createPublicKey(prepared.controllerPublicKeyPem));
      } finally { plaintext.fill(0); }
    });
  }
  beginDrain(identity: ElasticWorkerIdentity, operationId: string) {
    return this.guarded(() => { this.requireState(identity); return this.control.beginDrain(identity, operationId); });
  }
  private probe(checkpoint: WorkerCheckpoint | null = null) {
    // Include archived nodes; unknown cwd is an inventory failure, never an empty root list.
    const nodes = this.control.db.prepare('SELECT cwd FROM nodes').all() as Array<{ cwd: string | null }>;
    const cwds = nodes.map(node => node.cwd ?? '');
    const workspaces = this.control.db.prepare('SELECT workspace_json FROM rigs WHERE workspace_json IS NOT NULL').all() as Array<{ workspace_json: string }>;
    for (const row of workspaces) {
      const spec: unknown = JSON.parse(row.workspace_json);
      if (!object(spec) || typeof spec.workspaceRoot !== 'string' || !Array.isArray(spec.repos)) reject();
      cwds.push(spec.workspaceRoot);
      if (spec.knowledgeRoot != null) { if (typeof spec.knowledgeRoot !== 'string') reject(); cwds.push(spec.knowledgeRoot); }
      for (const repo of spec.repos) { if (!object(repo) || typeof repo.path !== 'string') reject(); cwds.push(repo.path); }
    }
    return inspectElasticWorker({ control: this.control, ownedRoots: this.manifest.ownedRoots, declaredCwds: cwds,
      processes: this.os.processes(), checkpoint });
  }
  checkpoint(identity: ElasticWorkerIdentity, generation: number): WorkerCheckpoint {
    return this.guarded(() => {
      const state = this.requireState(identity, generation); if (state.phase === 'ready') reject();
      const probe = this.probe(); if (!probe.ledger.clean || !probe.filesystem.complete || probe.filesystem.blockers.length) reject();
      return { digest: probe.filesystem.digest, files: probe.filesystem.files, refs: probe.filesystem.refs };
    });
  }
  /** Only call after the controller has independently persisted every listed file and Git object/ref. */
  acknowledgeCheckpoint(identity: ElasticWorkerIdentity, generation: number, checkpoint: WorkerCheckpoint): void {
    this.guarded(() => {
      this.requireState(identity, generation); const probe = this.probe(checkpoint);
      if (!probe.checkpointAcknowledged) reject(); this.persist('checkpoint.json', { identity, generation, checkpoint });
    });
  }
  seal(identity: ElasticWorkerIdentity, generation: number, operationId: string): AzureRemovalSeal {
    return this.guarded(() => {
      const state = this.requireState(identity, generation); if (state.phase === 'ready' || !state.drainStartedAt || !operationId || operationId.length > 512) reject();
      const saved = JSON.parse(readFileSync(this.path('checkpoint.json'), 'utf8')) as { identity: ElasticWorkerIdentity; generation: number; checkpoint: WorkerCheckpoint };
      if (workerDigest(saved.identity) !== workerDigest(identity) || saved.generation !== generation) reject();
      const before = this.probe(saved.checkpoint);
      const scope = (proof: WorkerProcessProof) => proof.uid === this.manifest.uid && proof.cgroup === `/system.slice/${this.manifest.unit}`;
      if (!before.clean || !scope(before.process)) reject();
      this.control.sealAdmission(identity, generation); this.os.installSealMarker(); this.os.stopUnit();
      const after = this.probe(saved.checkpoint); const stable = this.probe(saved.checkpoint);
      if (!after.clean || !stable.clean || !scope(after.process) || !scope(stable.process)
        || after.process.processes !== 0 || stable.process.processes !== 0
        || after.ledger.digest !== before.ledger.digest || stable.ledger.digest !== after.ledger.digest
        || after.filesystem.digest !== before.filesystem.digest || stable.filesystem.digest !== after.filesystem.digest) reject();
      const receipt: AzureRemovalSeal = { workerId: identity.workerId, vmId: identity.vmId, operationId,
        observedAt: stable.observedAt, drainConfirmedAt: state.drainStartedAt, drainGeneration: String(generation),
        beginDrainConfirmed: true, fenced: true, active: 0, claims: 0, processes: 0, pendingTransfers: false,
        pendingActivationIntents: false, coordinatorCheckpointAcknowledged: true, clean: true, pendingHumanReturns: false };
      this.persist('seal.json', receipt); return receipt;
    });
  }
}
