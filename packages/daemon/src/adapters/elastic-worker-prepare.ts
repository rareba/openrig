import { execFile } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readFileSync, realpathSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { z } from 'zod';
import type { BootstrapOrchestrator } from '../domain/bootstrap-orchestrator.js';
import type { RigRepository } from '../domain/rig-repository.js';
import type { SessionRegistry } from '../domain/session-registry.js';
import type { QueueRepository } from '../domain/queue-repository.js';
import { assertElasticEffectAllowed } from '../domain/elastic-worker-guard.js';
import { procIdentity, type LinuxWorkerManifest } from './linux-privileged-worker-boundary.js';
import { durableWrite, noFollowRead, sha256 } from './elastic-worker-checkpoint.js';
import { enrollmentDigest } from './elastic-worker-attempts.js';
import { repositoryPlanSchema, GithubCredentialStore, credentialReadiness, type GithubMaterial } from './elastic-worker-github.js';
import { parsePiRunnerState, piSeatPaths } from './pi-runner-protocol.js';
import { remoteJsonRequest } from '../domain/hosts/remote-daemon-http.js';
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const seat = z.string().regex(/^[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+$/).max(80);
const ownerSchema = z.object({ qitemId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/), nonce: z.string().regex(/^[a-f0-9]{32,64}$/), sourceSession: z.string().min(3).max(256), originHostId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/) }).strict();
export const preparationPlanSchema = z.object({ version: z.literal(1), operationId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/), manifestDigest: hash, specDigest: hash,
    model: z.string().regex(/^[A-Za-z0-9_.:/-]{1,128}$/), approvedActionKeys: z.array(z.string().max(256)).max(32), repositoryAccess: repositoryPlanSchema,
    seats: z.array(z.object({ sessionName: seat, repository: z.object({ id: z.number().int().positive(), name: z.string().regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/), baseRef: z.string().regex(/^refs\/heads\/[A-Za-z0-9_/-]+$/).refine(v => !v.includes('..') && !v.endsWith('/')), sha: z.string().regex(/^[a-f0-9]{40}$/) }).strict() }).strict()).min(1).max(4).refine(seats => new Set(seats.map(s => s.sessionName)).size === seats.length), owner: z.object({ sourceSession: z.string().min(3).max(256), originHostId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/), tasks: z.array(z.object({ sessionName: seat, qitemId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/), nonce: z.string().regex(/^[a-f0-9]{32,64}$/) }).strict()).min(1).max(4) }).strict().nullable() }).strict();
export type WorkerPreparationPlan = z.infer<typeof preparationPlanSchema>;
export interface PreparationSeatEvidence {
    sessionName: string;
    nodeId: string | null;
    generation: string | null;
    cwd: string;
    runtime: 'pi';
    model: string;
    launched: boolean;
    providerReady: boolean;
    toolProven: boolean;
    active: boolean | null;
}
export interface WorkerPreparationEvidence {
    version: 1;
    observedAt: number;
    manifestDigest: string;
    operationId: string;
    planDigest: string;
    state: 'pending' | 'ready' | 'blocked' | 'unknown';
    rung: 'enrolled' | 'launched' | 'provider-ready' | 'tool-proven';
    rigId: string | null;
    seats: PreparationSeatEvidence[];
    repositoryCapabilities: Array<{
        repositoryId: number;
        publicRead: boolean;
        privateRead: boolean;
        write: boolean;
    }>;
    publicReason: string;
}
export interface WorkerPreparationServices {
    bootstrap: BootstrapOrchestrator;
    rigs: RigRepository;
    sessions: SessionRegistry;
    queue: QueueRepository;
}
type NativeRun = (program: string, argv: string[], cwd: string, env: NodeJS.ProcessEnv) => Promise<string>;
function reject(): never { throw Error('elastic_preparation_rejected'); }
const defaultRun: NativeRun = (program, args, cwd, env) => new Promise((resolve, rejectPromise) => {
    execFile(program, args, { cwd, env, timeout: 60000, maxBuffer: 1024 * 1024, encoding: 'utf8', shell: false }, (error, stdout) => error ? rejectPromise(Error('elastic_preparation_unknown')) : resolve(stdout));
});
export function readinessProofArtifact(task: z.infer<typeof ownerSchema>, session: string, cwd: string, manifestDigest: string) {
    ownerSchema.parse(task);
    seat.parse(session);
    hash.parse(manifestDigest);
    if (!/^\/[A-Za-z0-9_./@-]+$/.test(cwd) || resolve(cwd) !== cwd)
        reject();
    const path = join(resolve(cwd, '..'), `proof-${task.qitemId}.json`);
    const bytes = JSON.stringify({ version: 1, qitemId: task.qitemId, nonce: task.nonce, sessionName: session, cwd, manifestDigest, originHostId: task.originHostId });
    return { path, bytes, sha256: sha256(Buffer.from(bytes)) };
}
/** Fixed command is both the task scope and the exact tool-call evidence expected in Pi history. */
export function readinessTaskBody(task: z.infer<typeof ownerSchema>, session: string, cwd: string, manifestDigest: string): string {
    const artifact = readinessProofArtifact(task, session, cwd, manifestDigest);
    return `cd '${cwd}' && rig whoami --json && rig queue show '${task.qitemId}' --full --json && (umask 077; set -C; printf '%s' '${artifact.bytes}' > '${artifact.path}') && /usr/bin/sha256sum '${artifact.path}' && printf '%s\\n' 'OPENRIG_ELASTIC_PROOF ${task.nonce} ${session} ${manifestDigest}'`;
}
/** Model-authored booleans never qualify; require a paired successful Bash result in native history. */
export function inspectPiToolProof(entries: unknown[], expected: {
    model: string;
    task: z.infer<typeof ownerSchema>;
    seat: string;
    cwd: string;
    manifestDigest: string;
}): boolean {
    const command = readinessTaskBody(expected.task, expected.seat, expected.cwd, expected.manifestDigest);
    const calls = new Set<string>();
    for (const input of entries) {
        const parsed = z.object({ type: z.literal('message'), message: z.record(z.string(), z.unknown()) }).passthrough().safeParse(input);
        if (!parsed.success)
            continue;
        const m = parsed.data.message;
        if (m.role === 'assistant' && (typeof m.provider === 'string' ? `${m.provider}/${m.model}` : m.model) === expected.model && Array.isArray(m.content)) {
            for (const item of m.content) {
                const call = z.object({ type: z.literal('toolCall'), id: z.string(), name: z.literal('bash'), arguments: z.object({ command: z.literal(command) }).passthrough() }).passthrough().safeParse(item);
                if (call.success)
                    calls.add(call.data.id);
            }
        }
        if (m.role === 'toolResult' && m.toolName === 'bash' && m.isError === false && typeof m.toolCallId === 'string' && calls.has(m.toolCallId) && Array.isArray(m.content)) {
            const text = m.content.map(c => z.object({ type: z.literal('text'), text: z.string() }).safeParse(c)).filter(p => p.success).map(p => p.data!.text).join('\n');
            const artifact = readinessProofArtifact(expected.task, expected.seat, expected.cwd, expected.manifestDigest);
            if (text.includes(`${artifact.sha256}  ${artifact.path}`) && text.includes(`OPENRIG_ELASTIC_PROOF ${expected.task.nonce} ${expected.seat} ${expected.manifestDigest}`))
                return true;
        }
    }
    return false;
}
interface PreparationRecord {
    version: 1;
    planDigest: string;
    operationId: string;
    phase: 'reserved' | 'cloning' | 'cloned' | 'launching' | 'launched';
    rigId: string | null;
}
/** This service executes only inside the enrolled, dedicated-UID service; no root project commands. */
export class ElasticWorkerPreparation {
    private active = false;
    private readonly plan: WorkerPreparationPlan;
    private readonly planDigest: string;
    private readonly directory: string;
    constructor(private readonly options: {
        manifest: LinuxWorkerManifest;
        manifestDigest: string;
        approvedPlan: WorkerPreparationPlan;
        services: WorkerPreparationServices;
        run?: NativeRun;
        githubFetch?: typeof fetch;
    }) {
        this.plan = preparationPlanSchema.parse(options.approvedPlan);
        this.planDigest = enrollmentDigest(this.plan);
        if (this.plan.owner && (this.plan.owner.tasks.length !== this.plan.seats.length || new Set(this.plan.owner.tasks.map(t => t.sessionName)).size !== this.plan.seats.length || this.plan.owner.tasks.some(t => !this.plan.seats.some(s => s.sessionName === t.sessionName))))
            reject();
        if (this.plan.manifestDigest !== options.manifestDigest || this.plan.seats.length !== options.manifest.linux.seats.length || this.plan.seats.some(s => !options.manifest.linux.seats.includes(s.sessionName)))
            reject();
        if (enrollmentDigest(this.plan.seats.map(s => s.sessionName)) !== enrollmentDigest(['worker-a@elastic', 'worker-b@elastic']))
            reject();
        this.directory = join(options.manifest.linux.home, 'elastic-preparation');
    }
    private assertWorker(allowDraining = false): void {
        if (process.getuid?.() !== this.options.manifest.uid || process.env.OPENRIG_DB !== this.options.manifest.linux.database)
            reject();
        assertElasticEffectAllowed(this.options.services.rigs.db, { allowDraining });
    }
    private record(): PreparationRecord | null {
        const path = join(this.directory, 'record.json');
        if (!existsSync(path))
            return null;
        const value = z.object({ version: z.literal(1), planDigest: hash, operationId: z.string(), phase: z.enum(['reserved', 'cloning', 'cloned', 'launching', 'launched']), rigId: z.string().nullable() }).strict().parse(JSON.parse(noFollowRead(path, 16384).toString()));
        if (value.planDigest !== this.planDigest || value.operationId !== this.plan.operationId)
            reject();
        return value;
    }
    private persist(record: PreparationRecord): void { durableWrite(join(this.directory, 'record.json'), JSON.stringify(record)); }
    private workspace(sessionName: string): string { return join(this.options.manifest.ownedRoots[0]!, 'workspaces', this.planDigest.slice(0, 24), sessionName); }
    private environment(): NodeJS.ProcessEnv {
        const m = this.options.manifest;
        return { PATH: '/opt/node/bin:/usr/bin:/bin', HOME: m.linux.home, TMPDIR: m.linux.temp, GIT_TERMINAL_PROMPT: '0', GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null', GIT_NO_LAZY_FETCH: '1', GIT_ALLOW_PROTOCOL: 'https', GIT_SSH_COMMAND: '/usr/bin/false' };
    }
    private git(args: string[], cwd: string): Promise<string> {
        const helper = this.plan.repositoryAccess.mode === 'token' ? '!/opt/node/bin/gh auth git-credential' : '';
        return (this.options.run ?? defaultRun)('/usr/bin/git', ['-c', 'core.hooksPath=/dev/null', '-c', 'core.fsmonitor=false', '-c', 'credential.helper=', '-c', `credential.https://github.com.helper=${helper}`, ...args], cwd, this.environment());
    }
    private async metadata(repository: WorkerPreparationPlan['seats'][number]['repository'], material: GithubMaterial | null): Promise<void> {
        const access = this.plan.repositoryAccess;
        if (access.mode === 'token') {
            if (!material || material.expiresAt <= Date.now() || enrollmentDigest(material.repositories) !== enrollmentDigest(access.repositories) || !access.repositories.some(r => r.id === repository.id && r.name === repository.name))
                reject();
        }
        const response = await remoteJsonRequest({ id: 'github', transport: 'http', url: 'https://api.github.com' }, `/repos/${repository.name}`, { method: 'GET', timeoutMs: 10000, maxBytes: 65536, redirect: 'error', fetchImpl: this.options.githubFetch,
            headers: { Accept: 'application/vnd.github+json', 'User-Agent': 'openrig-elastic' }, resolvedBearer: { ok: true, token: access.mode === 'token' ? material!.token : undefined } });
        if (!response.ok)
            reject();
        const repo = z.object({ id: z.number().int(), full_name: z.string(), private: z.boolean(), permissions: z.object({ pull: z.boolean().optional(), push: z.boolean().optional() }).optional() }).passthrough().parse(response.payload);
        if (repo.id !== repository.id || repo.full_name.toLowerCase() !== repository.name.toLowerCase() || (access.mode === 'public-readonly' && repo.private) || (access.mode === 'token' && repo.permissions?.pull !== true))
            reject();
        const approved = access.mode === 'token' ? access.repositories.find(r => r.id === repo.id) : null;
        if (approved?.permissions.contents === 'write' && repo.permissions?.push !== true)
            reject();
    }
    private async refreshRepositoryAccess(): Promise<void> {
        const path = join(this.directory, 'repository-access.json');
        durableWrite(path, JSON.stringify({ version: 1, planDigest: this.planDigest, state: 'unknown' }));
        const material = this.plan.repositoryAccess.mode === 'token' ? new GithubCredentialStore().read() : null;
        for (const seat of this.plan.seats)
            await this.metadata(seat.repository, material);
        durableWrite(path, JSON.stringify({ version: 1, planDigest: this.planDigest, state: 'verified', verifiedAt: Date.now(), revision: material?.revision ?? null, repositoryIds: [...new Set(this.plan.seats.map(s => s.repository.id))].sort() }));
    }
    private async checkout(): Promise<void> {
        for (const s of this.plan.seats) {
            const cwd = this.workspace(s.sessionName);
            const parent = resolve(cwd, '..');
            if (!existsSync(parent))
                mkdirSync(parent, { recursive: true, mode: 0o700 });
            if (realpathSync(parent) !== parent || lstatSync(parent).uid !== this.options.manifest.uid)
                reject();
            const remote = `https://github.com/${s.repository.name}.git`;
            if (!existsSync(cwd)) {
                await this.git(['clone', '--no-checkout', '--', remote, cwd], parent);
                await this.git(['fetch', '--no-tags', 'origin', s.repository.baseRef], cwd);
                if ((await this.git(['rev-parse', 'FETCH_HEAD'], cwd)).trim() !== s.repository.sha)
                    reject();
                await this.git(['checkout', '-b', `elastic/${this.plan.operationId}/${s.sessionName.replace('@', '-')}`, s.repository.sha, '--'], cwd);
            }
            if (realpathSync(cwd) !== cwd || lstatSync(cwd).uid !== this.options.manifest.uid || !lstatSync(join(cwd, '.git')).isDirectory())
                reject();
            if ((await this.git(['remote', 'get-url', 'origin'], cwd)).trim() !== remote || (await this.git(['rev-parse', 'HEAD'], cwd)).trim() !== s.repository.sha || (await this.git(['branch', '--show-current'], cwd)).trim() !== `elastic/${this.plan.operationId}/${s.sessionName.replace('@', '-')}` || (await this.git(['status', '--porcelain'], cwd)).trim())
                reject();
            if (existsSync(join(cwd, '.gitmodules')) || (await this.git(['ls-files', '--stage'], cwd)).split('\n').some(line => line.startsWith('160000 ')))
                reject();
            for (const attr of ['.gitattributes', '.git/info/attributes'])
                if (existsSync(join(cwd, attr)) && /filter\s*=\s*lfs/.test(noFollowRead(join(cwd, attr), 65536).toString()))
                    reject();
        }
    }
    private materialize(): string {
        const specPath = join(this.options.manifest.linux.sourceRoot, 'integrations/azure-elastic/worker.rig.yaml');
        let spec = noFollowRead(specPath, 65536).toString();
        if (sha256(Buffer.from(spec)) !== this.plan.specDigest || this.plan.seats.length !== 2)
            reject();
        for (const [index, s] of this.plan.seats.entries())
            spec = spec.replace(`__OPENRIG_CWD_${index === 0 ? 'A' : 'B'}__`, JSON.stringify(this.workspace(s.sessionName)));
        spec = spec.replaceAll('__OPENRIG_MODEL__', JSON.stringify(this.plan.model));
        if (/__OPENRIG_/.test(spec))
            reject();
        const path = join(this.directory, 'worker.rig.yaml');
        if (existsSync(path)) {
            if (noFollowRead(path, 65536).toString() !== spec)
                reject();
        }
        else
            durableWrite(path, spec);
        return path;
    }
    async prepare(request: {
        operationId: string;
        planDigest: string;
    }): Promise<WorkerPreparationEvidence> {
        this.assertWorker();
        if (request.operationId !== this.plan.operationId || request.planDigest !== this.planDigest || this.active)
            reject();
        this.active = true;
        try {
            if (!existsSync(this.directory))
                mkdirSync(this.directory, { mode: 0o700 });
            if (realpathSync(this.directory) !== this.directory || lstatSync(this.directory).uid !== this.options.manifest.uid || (lstatSync(this.directory).mode & 0o077))
                reject();
            let record = this.record();
            await this.refreshRepositoryAccess();
            if (record?.phase === 'launching' || record?.phase === 'launched')
                return this.inspect();
            if (record?.phase === 'cloning')
                return { ...this.inspect(), state: 'unknown', publicReason: 'checkout_outcome_unknown' };
            record = record ?? { version: 1, planDigest: this.planDigest, operationId: this.plan.operationId, phase: 'reserved', rigId: null };
            this.persist(record);
            if (record.phase === 'reserved') {
                record.phase = 'cloning';
                this.persist(record);
                await this.checkout();
                record.phase = 'cloned';
                this.persist(record);
            }
            const spec = this.materialize();
            const plan = await this.options.services.bootstrap.bootstrap({ mode: 'plan', sourceRef: spec, autoApprove: false });
            if (plan.status === 'failed' || plan.errors.length || enrollmentDigest([...(plan.actionKeys ?? [])].sort()) !== enrollmentDigest([...this.plan.approvedActionKeys].sort()))
                reject();
            if (this.options.services.rigs.findUnarchivedRigsByName('elastic').length)
                reject();
            record.phase = 'launching';
            this.persist(record);
            const applied = await this.options.services.bootstrap.bootstrap({ mode: 'apply', sourceRef: spec, autoApprove: false, approvedActionKeys: this.plan.approvedActionKeys });
            if (!applied.rigId || applied.status !== 'completed')
                return { ...this.inspect(), state: 'unknown', publicReason: 'native_launch_outcome_unknown' };
            record.rigId = applied.rigId;
            record.phase = 'launched';
            this.persist(record);
            return this.inspect();
        }
        catch {
            return { ...this.empty(), state: 'blocked', publicReason: 'preparation_prerequisite_unavailable' };
        }
        finally {
            this.active = false;
        }
    }
    private empty(): WorkerPreparationEvidence { return { version: 1, observedAt: Date.now(), manifestDigest: this.options.manifestDigest, operationId: this.plan.operationId, planDigest: this.planDigest, state: 'pending', rung: 'enrolled', rigId: null, seats: [], repositoryCapabilities: [], publicReason: 'native_preparation_pending' }; }
    inspect(): WorkerPreparationEvidence {
        this.assertWorker(true);
        const evidence = this.empty();
        const record = this.record();
        if (!record || !['launching', 'launched'].includes(record.phase))
            return evidence;
        const candidates = this.options.services.rigs.findUnarchivedRigsByName('elastic');
        const rig = record.rigId ? this.options.services.rigs.getRig(record.rigId) : candidates.length === 1 ? this.options.services.rigs.getRig(candidates[0]!.id) : null;
        if (!rig)
            return { ...evidence, state: 'unknown', publicReason: 'native_launch_outcome_unknown' };
        evidence.rigId = rig.rig.id;
        const sessions = this.options.services.sessions.getLatestLiveSessions(rig.rig.id);
        evidence.seats = this.plan.seats.map(s => {
            const current = sessions.filter(x => x.sessionName === s.sessionName);
            const native = current.length === 1 ? current[0] : undefined;
            const node = rig.nodes.find(n => n.id === native?.nodeId);
            const generation = native ? this.options.services.sessions.currentOccupantGenerationForSession(s.sessionName) : null;
            const cwd = this.workspace(s.sessionName);
            const launched = !!native && !!generation && node?.runtime === 'pi' && node.model === this.plan.model && node.cwd === cwd && native.cwd === cwd && native.status !== 'unknown';
            let providerReady = false;
            let toolProven = false;
            let active: boolean | null = null;
            const paths = piSeatPaths(this.options.manifest.linux.stateRoot, s.sessionName);
            try {
                const runner = parsePiRunnerState(noFollowRead(paths.runnerStatePath, 16384).toString());
                providerReady = launched && !!runner?.ready && !runner.exited && typeof runner.launchId === 'string' && !!runner.sessionFile && native?.resumeToken === runner.sessionFile;
                const proof = runner?.idleProof;
                if (providerReady && proof && proof.launchId === runner?.launchId && proof.at <= Date.now() && Date.now() - proof.at <= 5000) {
                    const parent = procIdentity(proof.runnerPid);
                    const child = procIdentity(proof.childPid);
                    const m = this.options.manifest;
                    const runnerEnvironment = Object.fromEntries(readFileSync(`/proc/${parent.pid}/environ`, 'utf8').split('\0').filter(Boolean).map(value => { const i = value.indexOf('='); return [value.slice(0, i), value.slice(i + 1)]; }));
                    if (parent.startTime === proof.runnerStartTime && child.startTime === proof.childStartTime && parent.uid === m.uid && child.uid === m.uid && child.ppid === parent.pid && parent.cgroup === `/system.slice/${m.unit}` && child.cgroup === parent.cgroup && parent.executable === m.linux.node && child.executable === m.linux.node && parent.argv[1] === join(resolve(m.linux.helper, '..'), 'pi-runner.js') && parent.argv[parent.argv.indexOf('--launch-id') + 1] === runner?.launchId && child.argv[1] === m.linux.pi && runnerEnvironment.OPENRIG_OCCUPANT_GENERATION === generation && runnerEnvironment.OPENRIG_NODE_ID === native?.nodeId && runnerEnvironment.OPENRIG_SESSION_NAME === s.sessionName)
                        active = !proof.idle;
                }
                providerReady = providerReady && active !== null;
                const owner = this.plan.owner;
                const assigned = owner?.tasks.find(t => t.sessionName === s.sessionName);
                const proofTask = owner && assigned ? { sourceSession: owner.sourceSession, originHostId: owner.originHostId, qitemId: assigned.qitemId, nonce: assigned.nonce } : null;
                if (providerReady && proofTask && runner?.sessionFile && runner.sessionFile.startsWith(paths.sessionsDir + '/') && realpathSync(runner.sessionFile) === runner.sessionFile) {
                    const task = this.options.services.queue.getById(proofTask.qitemId);
                    const expectedSource = `${proofTask.sourceSession}@${proofTask.originHostId}`;
                    if (task?.state === 'done' && task.claimedAt && task.sourceSession === expectedSource && task.destinationSession === s.sessionName && task.body === readinessTaskBody(proofTask, s.sessionName, cwd, this.options.manifestDigest)) {
                        const entries = noFollowRead(runner.sessionFile, 8 * 1024 * 1024).toString().split('\n').filter(Boolean).map(line => JSON.parse(line));
                        const artifact = readinessProofArtifact(proofTask, s.sessionName, cwd, this.options.manifestDigest);
                        const actual = noFollowRead(artifact.path, 8192);
                        toolProven = actual.toString() === artifact.bytes && sha256(actual) === artifact.sha256 && inspectPiToolProof(entries, { model: this.plan.model, task: proofTask, seat: s.sessionName, cwd, manifestDigest: this.options.manifestDigest });
                    }
                }
            }
            catch { /* Missing/stale evidence never creates capacity. */ }
            return { sessionName: s.sessionName, nodeId: native?.nodeId ?? null, generation, cwd, runtime: 'pi', model: this.plan.model, launched, providerReady, toolProven, active };
        });
        if (rig.nodes.length !== this.plan.seats.length)
            return { ...evidence, state: 'blocked', publicReason: 'finite_native_identity_mismatch' };
        if (evidence.seats.every(s => s.launched))
            evidence.rung = 'launched';
        if (evidence.seats.every(s => s.providerReady))
            evidence.rung = 'provider-ready';
        if (evidence.seats.every(s => s.toolProven))
            evidence.rung = 'tool-proven';
        let credentialsValid = this.plan.repositoryAccess.mode === 'public-readonly';
        let material: GithubMaterial | null = null;
        if (!credentialsValid) {
            try {
                material = new GithubCredentialStore().read();
                credentialsValid = credentialReadiness(this.plan.repositoryAccess, material, Date.now()).state === 'material-present-unverified';
            }
            catch {
                credentialsValid = false;
            }
        }
        try {
            const access = z.object({ version: z.literal(1), planDigest: hash, state: z.literal('verified'), verifiedAt: z.number().int(), revision: z.string().nullable(), repositoryIds: z.array(z.number().int()) }).strict().parse(JSON.parse(noFollowRead(join(this.directory, 'repository-access.json'), 16384).toString()));
            credentialsValid = credentialsValid && access.planDigest === this.planDigest && access.verifiedAt <= Date.now() && Date.now() - access.verifiedAt <= 60000 && access.revision === (material?.revision ?? null) && enrollmentDigest(access.repositoryIds) === enrollmentDigest([...new Set(this.plan.seats.map(s => s.repository.id))].sort());
        }
        catch {
            credentialsValid = false;
        }
        const ready = evidence.rung === 'tool-proven' && credentialsValid;
        evidence.state = ready ? 'ready' : 'pending';
        evidence.publicReason = ready ? 'native_tool_proof_complete' : !credentialsValid ? 'repository_access_unavailable' : 'native_tool_proof_pending';
        evidence.repositoryCapabilities = this.plan.seats.map(s => ({ repositoryId: s.repository.id, publicRead: ready && this.plan.repositoryAccess.mode === 'public-readonly', privateRead: ready && this.plan.repositoryAccess.mode === 'token', write: ready && this.plan.repositoryAccess.mode === 'token' && this.plan.repositoryAccess.repositories.some(r => r.id === s.repository.id && r.permissions.contents === 'write') }));
        return evidence;
    }
}
