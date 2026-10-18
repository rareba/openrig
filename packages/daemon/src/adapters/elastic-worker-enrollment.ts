import { piSeatPaths } from './pi-runner-protocol.js';
import { capacitySchema, accountWorkerCapacity } from './elastic-worker-capacity.js';
import { createHash, createPrivateKey, createPublicKey } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { z } from 'zod';
import { type AzureMutationContext, type AzureNativeContext, type AzureInstance } from './azure-elastic-fleet.js';
import { ElasticWorkerAttemptStore, enrollmentDigest, type ElasticWorkerAttempt, type AttemptIdentity } from './elastic-worker-attempts.js';
import { AzureWorkerCommandClient, type PinnedWorkerScript, type ManagedWorkerOperation, type TrustedWorkerEndpoint } from './azure-worker-command.js';
import { parseLinuxManifest, type LinuxWorkerManifest } from './linux-privileged-worker-boundary.js';
import { openEnrollment, sealGithubMaterial, sealWorkerMaterial } from '../domain/elastic-enrollment.js';
import { readWorkerRuntimeMaterial } from './azure-worker-bootstrap.js';
import { noFollowRead } from './elastic-worker-checkpoint.js';
import { githubMaterialSchema, readGithubCredentialSource, type GithubMaterial } from './elastic-worker-github.js';
import { preparationPlanSchema, type WorkerPreparationPlan } from './elastic-worker-prepare.js';
import { remoteJsonRequest } from '../domain/hosts/remote-daemon-http.js';
import { addHostEntry } from '../domain/hosts/hosts-registry-writer.js';
import { loadHostRegistry, validateHostRegistry } from '../domain/hosts/hosts-registry-reader.js';
import type { FleetWorkerObservation } from '../domain/elastic-fleet-policy.js';
import type { FleetOperation } from '../domain/elastic-fleet.js';
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const count = z.number().int().nonnegative();
const time = z.number().int().nonnegative();
const stateSchema = z.object({ version: z.literal(1), workerId: z.string().min(1), vmId: z.string().uuid(), manifestDigest: hash, phase: z.enum(['ready', 'draining', 'sealed']), generation: count, drainOperationId: z.string().nullable(), drainStartedAt: time.nullable() }).strict();
const preparationSchema = z.object({ version: z.literal(1), observedAt: time, manifestDigest: hash, operationId: z.string(), planDigest: hash, state: z.enum(['pending', 'ready', 'blocked', 'unknown']), rung: z.enum(['enrolled', 'launched', 'provider-ready', 'tool-proven']), rigId: z.string().nullable(), seats: z.array(z.object({ sessionName: z.string(), repositoryId: z.number().int().positive(), nodeId: z.string().nullable(), generation: z.string().nullable(), cwd: z.string(), runtime: z.literal('pi'), model: z.string(), launched: z.boolean(), providerReady: z.boolean(), toolProven: z.boolean(), active: z.boolean().nullable() }).strict()).max(4), repositoryCapabilities: z.array(z.object({ repositoryId: z.number().int().positive(), publicRead: z.boolean(), privateRead: z.boolean(), write: z.boolean() }).strict()).max(4), publicReason: z.string().max(128), capacity: capacitySchema.optional() }).strict();
const probeSchema = z.object({ version: z.literal(1), observedAt: time, selfHostId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/), build: z.object({ commit: z.string().regex(/^[a-f0-9]{40}$/), dirty: z.literal(false) }).passthrough(), state: stateSchema,
    ledger: z.object({ complete: z.boolean(), custody: count, runnable: count, claims: count, unresolvedOutbox: count, pendingHumanReturns: z.boolean(), pendingActivationIntents: z.boolean(), pendingTransfers: z.boolean(), clean: z.boolean(), digest: hash }).strict(),
    filesystem: z.object({ complete: z.boolean(), blockers: z.array(z.string()).max(1024), files: z.array(z.object({ path: z.string(), bytes: count, sha256: hash }).strict()).max(100000), refs: z.array(z.object({ repo: z.string(), ref: z.string(), sha: z.string() }).strict()).max(100000), digest: hash }).passthrough(),
    process: z.object({ observedAt: time, complete: z.boolean(), idle: z.boolean(), processes: count, uid: z.number().int().positive(), cgroup: z.literal('/system.slice/openrig-elastic.service') }).strict(), checkpointAcknowledged: z.boolean(), drainEligible: z.boolean().nullable().optional(), clean: z.boolean(), preparation: preparationSchema }).strict();
export type ManagedWorkerProbe = z.infer<typeof probeSchema>;
export function managedHostId(workerId: string): string { return `elastic-${enrollmentDigest(workerId).slice(0, 24)}`; }
function reject(): never { throw Error('elastic_enrollment_rejected'); }
export function parseManagedWorkerProbe(value: unknown, attempt: Pick<ElasticWorkerAttempt, 'workerId'> & {
    binding: Pick<ElasticWorkerAttempt['binding'], 'vmId' | 'manifestDigest'>;
}, commit: string, now: number): ManagedWorkerProbe {
    const p = probeSchema.parse(value);
    if (p.state.workerId !== attempt.workerId || p.state.vmId !== attempt.binding.vmId || p.state.manifestDigest !== attempt.binding.manifestDigest || p.preparation.manifestDigest !== attempt.binding.manifestDigest || p.build.commit !== commit
        || [p.observedAt, p.process.observedAt, p.preparation.observedAt, ...(p.preparation.capacity ? [p.preparation.capacity.observedAt] : [])].some(at => at > now || now - at > 5000))
        reject();
    return p;
}
export interface ElasticArtifactDescriptor {
    version: 1;
    url: string;
    privateIPv4: string;
    archiveSha256: string;
    archiveBytes: number;
    caPem: string;
    caSha256: string;
    certificateSha256?: string;
}
export interface UbuntuPackageRecipe {
    distribution: 'ubuntu';
    release: '24.04';
    architecture: 'amd64';
    basePackages: Record<string, string[]>;
    baseInventorySha256?: string;
    packages: Array<{
        file: string;
        package: string;
        version: string;
        architecture: 'amd64' | 'all';
        sha256: string;
        bytes: number;
    }>;
    finalTools: Array<{
        path: string;
        sha256: string;
    }>;
    signedMetadata: {
        keyringFile: string;
        keyringSha256: string;
        releases: Array<{
            file: string;
            sha256: string;
            indexes: Array<{
                file: string;
                sha256: string;
                releasePath: string;
            }>;
        }>;
    };
}
export interface ElasticEnrollmentOptions {
    store: ElasticWorkerAttemptStore;
    commands: AzureWorkerCommandClient;
    manifestRaw: string;
    preparationPlan: WorkerPreparationPlan;
    fleetEpoch: string;
    workerBuildCommit: string;
    artifact: ElasticArtifactDescriptor;
    packageRecipe: UbuntuPackageRecipe;
    checkpointCaPem: string;
    stageScript: PinnedWorkerScript;
    helperScript: PinnedWorkerScript;
    registryPath: string;
    fetchImpl?: typeof fetch;
    now?: () => number;
    assertMutation?: (context: AzureMutationContext) => void;
}
type EnrollmentContext = Pick<AzureMutationContext, 'configuration' | 'instance'> & {
    operation: Pick<FleetOperation, 'operationId' | 'type'>;
};
export interface EnrollmentResult {
    state: 'ready' | 'pending' | 'blocked' | 'unknown';
    publicReason: string;
}
export interface ManagedWorkerObservation {
    idleCustodyDigest?: string;
    worker: FleetWorkerObservation;
    probe: ManagedWorkerProbe | null;
    drainEligible: boolean | null;
    publicReason: string;
}
/** Concrete default consumer; activation, scheduler and stopped-owner recovery are separately composed. */
export class ElasticWorkerEnrollment {
    private readonly manifest: LinuxWorkerManifest;
    private readonly manifestDigest: string;
    private readonly plan: WorkerPreparationPlan;
    private readonly configurationDigest: string;
    private readonly active = new Set<string>();
    constructor(private readonly options: ElasticEnrollmentOptions) {
        this.manifest = parseLinuxManifest(options.manifestRaw);
        this.manifestDigest = createHash('sha256').update(options.manifestRaw).digest('hex');
        this.plan = preparationPlanSchema.parse(options.preparationPlan);
        if (this.plan.manifestDigest !== this.manifestDigest || !/^[a-f0-9]{40}$/.test(options.workerBuildCommit))
            reject();
        this.configurationDigest = enrollmentDigest({ manifest: this.manifestDigest, plan: this.plan, artifact: options.artifact, recipe: options.packageRecipe, stage: options.stageScript, helper: options.helperScript, checkpointCaPem: options.checkpointCaPem, epoch: options.fleetEpoch, commit: options.workerBuildCommit });
    }
    private identity(context: EnrollmentContext): AttemptIdentity {
        if (context.configuration.resourceId !== this.manifest.resourceId || context.configuration.bootstrap.digest !== this.manifestDigest || context.operation.type !== 'provision')
            reject();
        return { resourceId: context.configuration.resourceId, childResourceId: context.instance.resourceId, instanceId: context.instance.instanceId, vmId: context.instance.vmId,
            operationId: context.operation.operationId, fleetEpoch: this.options.fleetEpoch, manifestDigest: this.manifestDigest, artifactDigest: this.options.artifact.archiveSha256,
            specDigest: this.plan.specDigest, checkoutDigest: enrollmentDigest(this.plan.seats), configurationDigest: this.configurationDigest };
    }
    private id(context: EnrollmentContext): string { const i = this.identity(context); return enrollmentDigest([i.resourceId.toLowerCase(), i.vmId.toLowerCase(), i.operationId, i.fleetEpoch]); }
    private check(a: ElasticWorkerAttempt, context: EnrollmentContext): void { if (enrollmentDigest(a.identity) !== enrollmentDigest(this.identity(context)))
        reject(); }
    private checkAttempt(a: ElasticWorkerAttempt): void {
        if (this.options.store.readPrivateInput(a.id, 'registration-retirement-intent')) reject();
        if (a.identity.configurationDigest !== this.configurationDigest || a.identity.fleetEpoch !== this.options.fleetEpoch || a.binding.manifestDigest !== this.manifestDigest
            || a.identity.resourceId !== this.manifest.resourceId || a.identity.specDigest !== this.plan.specDigest || a.identity.checkoutDigest !== enrollmentDigest(this.plan.seats)) reject();
    }
    private async trusted(a: ElasticWorkerAttempt, configuration: AzureMutationContext['configuration']): Promise<TrustedWorkerEndpoint> {
        this.checkAttempt(a);
        const instance: AzureInstance = { resourceId: a.identity.childResourceId, instanceId: a.identity.instanceId, vmId: a.binding.vmId, workerId: a.workerId, protected: true, provisioningState: 'Succeeded' };
        const current = await this.options.commands.resolveTrustedInstance(configuration, instance, a.binding, { port: this.manifest.port, imageReference: this.manifest.linux.imageReference });
        const saved = this.options.store.readPrivateInput(a.id, 'trusted-endpoint');
        if (saved !== null && enrollmentDigest(saved) !== enrollmentDigest(current))
            reject();
        return current;
    }
    private async rpc(a: ElasticWorkerAttempt, endpoint: TrustedWorkerEndpoint, method: 'GET' | 'POST', path: string, body?: unknown) {
        const response = await remoteJsonRequest({ id: managedHostId(a.workerId), transport: 'http', url: endpoint.baseUrl, bearer_file: this.options.store.tokenPath(a.id) }, path, { method, body, timeoutMs: 10000, maxBytes: 4 * 1024 * 1024, redirect: 'error', fetchImpl: this.options.fetchImpl });
        if (!response.ok)
            throw Error('elastic_worker_native_unknown');
        return response.payload;
    }
    private async submit(a: ElasticWorkerAttempt, context: AzureMutationContext, operation: ManagedWorkerOperation, payload: unknown): Promise<EnrollmentResult> {
        const endpoint = await this.trusted(a, context.configuration);
        this.options.assertMutation?.(context);
        this.options.store.savePrivateInput(a.id, 'trusted-endpoint', endpoint);
        const script = operation === 'stage' ? this.options.stageScript : this.options.helperScript;
        const identity = this.options.commands.commandIdentity(a, operation, payload, script);
        const pending = this.options.store.advance(a.id, a.revision, { command: { name: identity.name, operation, requestDigest: identity.requestDigest, state: 'intent' }, phase: operation === 'stage' ? 'staging' : a.phase, publicReason: 'managed_command_pending' });
        // Persisted intent is never replayed after an uncertain create, including a missing subsequent GET.
        const state = await this.options.commands.submitManagedCommand(pending, operation, { configuration: context.configuration, endpoint, script, payload, assertDispatch: () => this.options.assertMutation?.(context) });
        this.options.store.advance(a.id, pending.revision, { command: { ...pending.command!, state }, publicReason: state === 'unknown' ? 'managed_command_unknown' : 'managed_command_pending' });
        return { state: state === 'unknown' ? 'unknown' : 'pending', publicReason: state === 'unknown' ? 'managed_command_unknown' : 'managed_command_pending' };
    }
    private decode(a: ElasticWorkerAttempt, output: string): ElasticWorkerAttempt {
        const command = a.command;
        if (!command)
            reject();
        if (command.operation === 'stage') {
            const result = z.object({ version: z.literal(1), installed: z.literal(true), manifestDigest: hash }).strict().parse(JSON.parse(output));
            if (result.manifestDigest !== this.manifestDigest)
                reject();
            return this.options.store.advance(a.id, a.revision, { phase: 'installed', command: { ...command, state: 'succeeded' } });
        }
        if (command.operation === 'prepare') {
            const result = z.object({ version: z.literal(1), binding: z.unknown(), publicKeyPem: z.string().max(8192) }).strict().parse(JSON.parse(output));
            if (enrollmentDigest(result.binding) !== enrollmentDigest(a.binding))
                reject();
            const key = createPublicKey(result.publicKeyPem);
            if (key.asymmetricKeyType !== 'rsa' || (key.asymmetricKeyDetails?.modulusLength ?? 0) < 3072)
                reject();
            return this.options.store.advance(a.id, a.revision, { phase: 'prepared', workerPublicKeyPem: result.publicKeyPem, command: { ...command, state: 'succeeded' } });
        }
        if (command.operation === 'enroll') {
            const key = this.options.store.privateKey(a.id);
            try {
                const bytes = openEnrollment(output.trimEnd(), a.binding, createPrivateKey(key));
                try {
                    this.options.store.saveBearer(a.id, bytes.toString());
                }
                finally {
                    bytes.fill(0);
                }
            }
            finally {
                key.fill(0);
            }
            return this.options.store.advance(a.id, a.revision, { phase: 'enrolled', command: { ...command, state: 'succeeded' } });
        }
        if (command.operation !== 'github-credential-install') reject();
        const receipt = z.object({ revision: z.string(), expiresAt: z.number().int().positive(), materialDigest: hash }).strict().parse(JSON.parse(output));
        const saved = z.object({ binding: z.unknown(), materialEnvelope: z.string(), revision: z.string(), expiresAt: z.number(), materialDigest: hash }).strict().parse(this.options.store.readPrivateInput(a.id, `github-${receipt.materialDigest}`));
        if (this.options.commands.commandIdentity(a, 'github-credential-install', { binding: saved.binding, materialEnvelope: saved.materialEnvelope }, this.options.helperScript).requestDigest !== command.requestDigest)
            reject();
        if (receipt.revision !== saved.revision || receipt.expiresAt !== saved.expiresAt || receipt.materialDigest !== saved.materialDigest)
            reject();
        return this.options.store.advance(a.id, a.revision, { credential: { revision: receipt.revision, expiresAt: receipt.expiresAt }, command: { ...command, state: 'succeeded' } });
    }
    async advanceEnrollment(context: AzureMutationContext): Promise<EnrollmentResult> {
        const id = this.id(context);
        if (this.active.has(id))
            return { state: 'pending', publicReason: 'enrollment_writer_active' };
        this.active.add(id);
        try {
            let a = this.options.store.createOrRead(this.identity(context));
            this.check(a, context);
            if (a.phase === 'blocked')
                return { state: 'blocked', publicReason: a.publicReason };
            if (a.command && a.command.state !== 'succeeded') {
                const observation = await this.options.commands.inspectManagedCommand(a, context.configuration);
                if (observation.state === 'unknown' || observation.state === 'pending')
                    return { state: observation.state === 'unknown' ? 'unknown' : 'pending', publicReason: 'managed_command_pending' };
                if (observation.state === 'failed') {
                    this.options.store.advance(a.id, a.revision, { phase: 'blocked', command: { ...a.command, state: 'failed' }, publicReason: 'managed_command_failed' });
                    return { state: 'blocked', publicReason: 'managed_command_failed' };
                }
                if (observation.state !== 'succeeded')
                    reject();
                a = this.decode(a, observation.output);
            }
            if (a.phase === 'reserved') {
                const payload = this.options.store.savePrivateInput(a.id, 'stage', { artifact: this.options.artifact, manifestRaw: this.options.manifestRaw, manifestDigest: this.manifestDigest, packageRecipe: this.options.packageRecipe, checkpointCaPem: this.options.checkpointCaPem, preparationPlan: this.plan });
                return await this.submit(a, context, 'stage', payload);
            }
            if (a.phase === 'installed')
                return await this.submit(a, context, 'prepare', { binding: a.binding, controllerPublicKeyPem: this.options.store.publicKey(a.id) });
            if (a.phase === 'prepared') {
                let payload = this.options.store.readPrivateInput(a.id, 'enroll');
                if (!payload) {
                    if (!a.workerPublicKeyPem)
                        reject();
                    const bytes = readWorkerRuntimeMaterial(this.manifest.materialFiles);
                    try {
                        payload = this.options.store.savePrivateInput(a.id, 'enroll', { binding: a.binding, materialEnvelope: sealWorkerMaterial(bytes, a.binding, createPublicKey(a.workerPublicKeyPem)) });
                    }
                    finally {
                        bytes.fill(0);
                    }
                }
                return await this.submit(a, context, 'enroll', payload);
            }
            if (['enrolled', 'registered', 'preparing-native', 'ready'].includes(a.phase)) {
                const endpoint = await this.trusted(a, context.configuration);
                const probe = parseManagedWorkerProbe(await this.rpc(a, endpoint, 'GET', '/api/elastic-worker/probe'), a, this.options.workerBuildCommit, (this.options.now ?? Date.now)());
                this.checkProbePlan(probe);
                a = await this.ensureElasticRegistration(a, context.configuration, probe);
                if (this.plan.repositoryAccess.mode === 'token' && !a.credential)
                    return await this.installGithub(a, context);
                if (probe.preparation.state !== 'ready') {
                    if (a.phase !== 'preparing-native')
                        a = this.options.store.advance(a.id, a.revision, { phase: 'preparing-native', publicReason: 'native_preparation_pending' });
                    // Worker stores its own launch intent; repeated POST is a read/reconciliation of the same plan.
                    this.options.assertMutation?.(context);
                    await this.rpc(a, endpoint, 'POST', '/api/elastic-worker/prepare', { operationId: this.plan.operationId, planDigest: enrollmentDigest(this.plan) });
                    return { state: 'pending', publicReason: 'native_tool_proof_pending' };
                }
                if (!this.schedulable(probe))
                    return { state: 'pending', publicReason: 'native_tool_proof_pending' };
                if (a.phase !== 'ready')
                    this.options.store.advance(a.id, a.revision, { phase: 'ready', publicReason: 'native_worker_ready' });
                return { state: 'ready', publicReason: 'native_worker_ready' };
            }
            return { state: 'unknown', publicReason: 'enrollment_reconciliation_required' };
        }
        catch {
            return { state: 'unknown', publicReason: 'enrollment_evidence_unavailable' };
        }
        finally {
            this.active.delete(id);
        }
    }
    private async installGithub(a: ElasticWorkerAttempt, context: AzureMutationContext): Promise<EnrollmentResult> {
        if (this.plan.repositoryAccess.mode !== 'token' || !a.workerPublicKeyPem)
            reject();
        const material = readGithubCredentialSource(this.plan.repositoryAccess.credentialSourceRef);
        if (material.revision !== this.plan.repositoryAccess.credentialRevision || material.expiresAt !== this.plan.repositoryAccess.expiresAt)
            reject();
        return this.submitGithubMaterial(a, context, material);
    }
    private async submitGithubMaterial(a: ElasticWorkerAttempt, context: AzureMutationContext, material: GithubMaterial): Promise<EnrollmentResult> {
        if (this.plan.repositoryAccess.mode !== 'token' || !a.workerPublicKeyPem || material.expiresAt <= Date.now() || enrollmentDigest(material.repositories) !== enrollmentDigest(this.plan.repositoryAccess.repositories) || material.expectedPreviousRevision !== (a.credential?.revision ?? null))
            reject();
        const materialDigest = enrollmentDigest(material);
        let saved = this.options.store.readPrivateInput(a.id, `github-${materialDigest}`);
        if (!saved) {
            const bytes = Buffer.from(JSON.stringify(material));
            try {
                saved = this.options.store.savePrivateInput(a.id, `github-${materialDigest}`, { binding: a.binding, materialEnvelope: sealGithubMaterial(bytes, a.binding, createPublicKey(a.workerPublicKeyPem)), revision: material.revision, expiresAt: material.expiresAt, materialDigest });
            }
            finally {
                bytes.fill(0);
            }
        }
        const parsed = z.object({ binding: z.unknown(), materialEnvelope: z.string() }).passthrough().parse(saved);
        return this.submit(a, context, 'github-credential-install', { binding: parsed.binding, materialEnvelope: parsed.materialEnvelope });
    }
    /** Explicit replacement source only. No signer, grant, home copy or automatic issuer is inferred. */
    async advanceGithubCredential(context: AzureMutationContext, source: {
        sourceRef: string;
        expectedPreviousRevision: string;
        revision: string;
        expiresAt: number;
    }): Promise<EnrollmentResult> {
        const id = this.id(context);
        if (this.active.has(id))
            return { state: 'pending', publicReason: 'enrollment_writer_active' };
        this.active.add(id);
        try {
            let a = this.options.store.read(id);
            this.check(a, context);
            await this.trusted(a, context.configuration);
            if (!a.registration || !['registered', 'preparing-native', 'ready'].includes(a.phase))
                reject();
            if (a.command && a.command.state !== 'succeeded') {
                const observed = await this.options.commands.inspectManagedCommand(a, context.configuration);
                if (observed.state !== 'succeeded')
                    return { state: observed.state === 'failed' ? 'blocked' : observed.state, publicReason: 'managed_command_readback' };
                a = this.decode(a, observed.output);
            }
            const material = readGithubCredentialSource(source.sourceRef);
            if (material.revision !== source.revision || material.expiresAt !== source.expiresAt || material.expectedPreviousRevision !== source.expectedPreviousRevision)
                reject();
            if (a.credential?.revision === source.revision && !this.options.store.readPrivateInput(a.id, `github-${enrollmentDigest(material)}`))
                reject();
            if (a.credential?.revision === source.revision)
                return { state: 'ready', publicReason: 'github_credential_installed' };
            return await this.submitGithubMaterial(a, context, material);
        }
        catch {
            return { state: 'blocked', publicReason: 'github_credential_prerequisite_unavailable' };
        }
        finally {
            this.active.delete(id);
        }
    }
    private checkProbePlan(probe: ManagedWorkerProbe): void {
        if (probe.preparation.operationId !== this.plan.operationId || probe.preparation.planDigest !== enrollmentDigest(this.plan) || probe.process.uid !== this.manifest.uid)
            reject();
        if (probe.preparation.seats.length && (probe.preparation.seats.length !== this.plan.seats.length || probe.preparation.seats.some(s => !this.plan.seats.some(p => p.sessionName === s.sessionName && s.model === this.plan.model && s.repositoryId === p.repository.id))))
            reject();
    }
    private currentCapacity(p: ManagedWorkerProbe): boolean {
        const c = p.preparation.capacity;
        return !!c?.complete && c.processes === c.inventory.length && accountWorkerCapacity(this.manifest, structuredClone(c.seats), c.inventory, c.inventory.find(p => p.executable === this.manifest.linux.node && p.argv[1] === this.manifest.linux.daemon)?.pid ?? -1, c.observedAt).complete && c.uid === this.manifest.uid && c.cgroup === `/system.slice/${this.manifest.unit}` && c.seats.length === this.plan.seats.length
          && new Set(c.seats.map(s => s.sessionName)).size === c.seats.length && c.seats.every(s => {
            const native = p.preparation.seats.find(n => n.sessionName === s.sessionName);
            return !!native && s.nodeId === native.nodeId && s.generation === native.generation && s.cwd === native.cwd && s.model === native.model && s.occupancy === (native.active === true ? 'occupied' : native.active === false ? 'free' : 'unknown')
              && s.occupancy !== 'unknown' && s.runner.uid === c.uid && s.child.uid === c.uid && s.runner.cgroup === c.cgroup && s.child.cgroup === c.cgroup
              && s.runner.executable === this.manifest.linux.node && s.child.executable === this.manifest.linux.node && s.child.argv[1] === this.manifest.linux.pi
              && s.child.ppid === s.runner.pid && (s.occupancy !== 'free' || s.descendants.length === 0);
          });
    }
    private schedulable(p: ManagedWorkerProbe): boolean {
        return p.state.phase === 'ready' && this.currentCapacity(p) && p.ledger.complete && p.preparation.state === 'ready' && p.preparation.rung === 'tool-proven' && p.preparation.seats.length === this.plan.seats.length && p.preparation.seats.every(s => s.launched && s.providerReady && s.toolProven && s.generation && s.active !== null);
    }
    async inspectEnrollment(context: EnrollmentContext): Promise<EnrollmentResult> {
        try {
            const a = this.options.store.read(this.id(context));
            this.check(a, context);
            if (a.phase === 'blocked')
                return { state: 'blocked', publicReason: a.publicReason };
            if (a.command && a.command.state !== 'succeeded') {
                const result = await this.options.commands.inspectManagedCommand(a, context.configuration);
                return { state: result.state === 'unknown' ? 'unknown' : result.state === 'failed' ? 'blocked' : 'pending', publicReason: 'managed_command_readback' };
            }
            const observation = await this.observeManagedWorker(a, context.configuration);
            return { state: observation.worker.state === 'ready' && a.phase === 'ready' ? 'ready' : 'pending', publicReason: observation.publicReason };
        }
        catch {
            return { state: 'unknown', publicReason: 'enrollment_evidence_unavailable' };
        }
    }
    async bootstrap(context: AzureMutationContext): Promise<void> { const result = await this.advanceEnrollment(context); if (result.state !== 'ready')
        throw Error(result.publicReason); }
    /** Native CLI/daemon writers serialize; external whole-file editor drift remains fail-closed. */
    async ensureElasticRegistration(attempt: ElasticWorkerAttempt, configuration: AzureMutationContext['configuration'], suppliedProbe?: ManagedWorkerProbe): Promise<ElasticWorkerAttempt> {
        const a = this.options.store.read(attempt.id);
        this.checkAttempt(a);
        const endpoint = await this.trusted(a, configuration);
        const probe = suppliedProbe ?? parseManagedWorkerProbe(await this.rpc(a, endpoint, 'GET', '/api/elastic-worker/probe'), a, this.options.workerBuildCommit, (this.options.now ?? Date.now)());
        this.checkProbePlan(probe);
        const id = managedHostId(a.workerId);
        const validated = validateHostRegistry({ hosts: [{ id, transport: 'http', hostId: probe.selfHostId, url: endpoint.baseUrl, bearer_file: this.options.store.tokenPath(a.id) }] }, this.options.registryPath);
        if (!validated.ok)
            reject();
        const entry = validated.registry.hosts[0]!;
        const before = existsSync(this.options.registryPath) ? readFileSync(this.options.registryPath) : null;
        const existing = before ? loadHostRegistry(this.options.registryPath) : null;
        if (existing && !existing.ok)
            reject();
        const current = existing?.ok ? existing.registry.hosts.find(h => h.id === id) : undefined;
        if (current && enrollmentDigest(current) !== enrollmentDigest(entry))
            reject();
        if (!current) {
            if ((before?.toString() ?? null) !== (existsSync(this.options.registryPath) ? readFileSync(this.options.registryPath, 'utf8') : null))
                reject();
            const result = addHostEntry({ ...entry }, this.options.registryPath);
            if (!result.ok)
                reject();
            const after = loadHostRegistry(this.options.registryPath);
            if (!after.ok || !after.registry.hosts.some(h => enrollmentDigest(h) === enrollmentDigest(entry)) || (existing?.ok && existing.registry.hosts.some(h => !after.registry.hosts.some(next => enrollmentDigest(h) === enrollmentDigest(next)))))
                reject();
        }
        const registration = { hostId: id, baseUrl: endpoint.baseUrl, selfHostId: probe.selfHostId };
        if (a.registration) {
            if (enrollmentDigest(a.registration) !== enrollmentDigest(registration))
                reject();
            return a;
        }
        return this.options.store.advance(a.id, a.revision, { registration, phase: 'registered', publicReason: 'native_registered' });
    }
    /** Recorded registration is an address book only; every network consumer resolves it afresh. */
    recordedEndpoint(attempt: ElasticWorkerAttempt) {
        const a = this.options.store.read(attempt.id);
        this.checkAttempt(a);
        if (!a.registration || a.registration.hostId !== managedHostId(a.workerId)) reject();
        const loaded = loadHostRegistry(this.options.registryPath);
        if (!loaded.ok) reject();
        const host = loaded.registry.hosts.find(h => h.id === a.registration!.hostId);
        if (!host || host.transport !== 'http' || host.hostId !== a.registration.selfHostId || host.url !== a.registration.baseUrl || host.bearer_file !== this.options.store.tokenPath(a.id) || host.bearer_env) reject();
        return host;
    }
    /** Fresh ARM, authenticated native identity and exact registered endpoint. No question may select it. */
    async registeredEndpoint(attempt: ElasticWorkerAttempt, configuration: AzureMutationContext['configuration']) {
        const a = this.options.store.read(attempt.id);
        if (!a.registration) reject();
        const endpoint = await this.trusted(a, configuration);
        const probe = parseManagedWorkerProbe(await this.rpc(a, endpoint, 'GET', '/api/elastic-worker/probe'), a, this.options.workerBuildCommit, (this.options.now ?? Date.now)());
        this.checkProbePlan(probe);
        if (a.registration.baseUrl !== endpoint.baseUrl || a.registration.selfHostId !== probe.selfHostId || a.registration.hostId !== managedHostId(a.workerId)) reject();
        const host = this.recordedEndpoint(a);
        if (host.url !== endpoint.baseUrl) reject();
        return { host, probe, attempt: a };
    }
    hostRegistry() { const loaded = loadHostRegistry(this.options.registryPath); if (!loaded.ok) reject(); return loaded.registry; }
    preparationPlan(): WorkerPreparationPlan { return preparationPlanSchema.parse(this.plan); }
    workerWorkspace(sessionName: string): string {
        if (!this.plan.seats.some(s => s.sessionName === sessionName)) reject();
        return `${this.manifest.ownedRoots[0]}/workspaces/${enrollmentDigest(this.plan).slice(0,24)}/${sessionName}`;
    }
    async observeManagedWorker(attempt: ElasticWorkerAttempt, configuration: AzureMutationContext['configuration']): Promise<ManagedWorkerObservation> {
        const a = this.options.store.read(attempt.id);
        const now = (this.options.now ?? Date.now)();
        const base: FleetWorkerObservation = { id: a.workerId, managed: true, permanent: false, state: a.phase === 'ready' ? 'quarantined' : 'provisioning', capabilities: [], slots: this.plan.seats.length, active: null, observedAt: now, idleSince: null, clean: null, pendingHumanReturns: null };
        try {
            const endpoint = await this.trusted(a, configuration);
            const probe = parseManagedWorkerProbe(await this.rpc(a, endpoint, 'GET', '/api/elastic-worker/probe'), a, this.options.workerBuildCommit, (this.options.now ?? Date.now)());
            this.checkProbePlan(probe);
            if (a.registration && (a.registration.baseUrl !== endpoint.baseUrl || a.registration.selfHostId !== probe.selfHostId))
                reject();
            const ready = this.schedulable(probe);
            const active = probe.preparation.seats.every(s => s.active !== null) ? probe.preparation.seats.filter(s => s.active).length : null;
            const capabilities = ready ? probe.preparation.repositoryCapabilities.flatMap(r => [r.publicRead ? `github:${r.repositoryId}:public-read` : '', r.privateRead ? `github:${r.repositoryId}:private-read` : '', r.write ? `github:${r.repositoryId}:write` : ''].filter(Boolean)) : [];
            const runtimeSidecars = new Set(this.plan.seats.map(s => piSeatPaths(this.manifest.linux.stateRoot, s.sessionName).runnerStatePath));
            const idleCustodyDigest = enrollmentDigest({ ledger: probe.ledger.digest, files: probe.filesystem.files.filter(f => !runtimeSidecars.has(f.path)), refs: probe.filesystem.refs });
            return { idleCustodyDigest, worker: { ...base, state: probe.state.phase === 'draining' ? 'draining' : ready && !!a.registration ? 'ready' : 'quarantined', capabilities, active, observedAt: probe.observedAt, clean: probe.drainEligible ?? null, pendingHumanReturns: probe.ledger.pendingHumanReturns }, probe, drainEligible: probe.drainEligible ?? null, publicReason: ready ? 'native_worker_ready' : 'native_tool_proof_pending' };
        }
        catch {
            return { worker: base, probe: null, drainEligible: null, publicReason: 'native_observation_unavailable' };
        }
    }
    async inspectNative(context: AzureNativeContext & {
        operation: FleetOperation;
    }): Promise<'succeeded' | 'pending' | 'unknown'> {
        if (context.operation.type !== 'provision')
            return 'unknown';
        const { operation, instances } = context;
        const previous = new Set(operation.snapshot.workers.filter(w => w.managed && !w.permanent).map(w => w.id));
        const added = instances.filter(i => !previous.has(i.workerId));
        const attempts = this.options.store.list().filter(a => a.identity.operationId === operation.operationId);
        if (instances.length !== operation.targetCapacity || operation.count < 1 || added.length !== operation.count
            || [...previous].some(id => !instances.some(i => i.workerId === id))
            || attempts.length !== added.length)
            return 'unknown';
        for (const key of ['workerId', 'vmId', 'resourceId', 'instanceId'] as const) {
            if (new Set(instances.map(i => i[key])).size !== instances.length)
                return 'unknown';
        }
        const matched = added.map(instance => ({ instance, attempts: attempts.filter(a => a.workerId === instance.workerId
                && a.binding.vmId === instance.vmId && a.identity.childResourceId === instance.resourceId
                && a.identity.instanceId === instance.instanceId) }));
        if (matched.some(item => item.attempts.length !== 1))
            return 'unknown';
        const states = await Promise.all(matched.map(({ instance }) =>
            this.inspectEnrollment({ configuration: context.configuration, instance, operation })));
        return states.length && states.every(s => s.state === 'ready') ? 'succeeded' : states.some(s => s.state === 'unknown') ? 'unknown' : 'pending';
    }
}
