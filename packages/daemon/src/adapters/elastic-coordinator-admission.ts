import type { FleetWorkerObservation } from '../domain/elastic-fleet-policy.js';
import { resolveHost } from '../domain/hosts/hosts-registry-reader.js';
import { createHash } from 'node:crypto';
import type { SessionRegistry } from '../domain/session-registry.js';
import type { ElasticHumanProxyAuthority, ElasticHumanWorker } from '../domain/elastic-human-proxy.js';
import type { GithubIntakeIO, GithubTask } from '../domain/github-intake.js';
import type { AzureMutationContext } from './azure-elastic-fleet.js';
import { ElasticFleetProjection } from '../domain/elastic-fleet-projection.js';
import { remoteJsonRequest } from '../domain/hosts/remote-daemon-http.js';
import { ElasticWorkerAttemptStore } from './elastic-worker-attempts.js';
import { ElasticWorkerEnrollment } from './elastic-worker-enrollment.js';
import { preparationPlanSchema, readinessTaskBody, type WorkerPreparationPlan } from './elastic-worker-prepare.js';
import type { EnrolledHumanOrigin } from './elastic-human-forwarder.js';
import { parseNativeHumanQuestion } from './elastic-human-forwarder.js';
/** Bind stable task identities before constructing enrollment. A real native registered owner is required. */
export function bindCoordinatorReadinessPlan(input: {
    plan: WorkerPreparationPlan;
    sessions: SessionRegistry;
    manager: string;
    hostId: string;
}): WorkerPreparationPlan {
    if (!input.sessions.currentOccupantGenerationForSession(input.manager) || !/^[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+$/.test(input.manager) || !/^[A-Za-z0-9_-]{1,128}$/.test(input.hostId))
        throw Error('native_readiness_owner_required');
    const tasks = input.plan.seats.map(s => { const digest = createHash('sha256').update(JSON.stringify([input.plan.operationId, input.plan.manifestDigest, input.manager, input.hostId, s.sessionName])).digest('hex'); return { sessionName: s.sessionName, qitemId: `qitem-elastic-proof-${digest.slice(0, 40)}`, nonce: digest }; });
    const owner = { sourceSession: input.manager, originHostId: input.hostId, tasks };
    if (input.plan.owner && JSON.stringify(input.plan.owner) !== JSON.stringify(owner))
        throw Error('native_readiness_owner_conflict');
    return preparationPlanSchema.parse({ ...input.plan, owner });
}
/** Concrete directory over original attempts and enrollment's trusted ARM/native/registry readback. */
export class ElasticEnrolledWorkerDirectory {
    private current = new Map<string, EnrolledHumanOrigin>();
    private managers = new Map<string, Set<string>>();
    constructor(readonly options: {
        store: ElasticWorkerAttemptStore;
        enrollment: ElasticWorkerEnrollment;
        configuration: () => AzureMutationContext['configuration'];
        sessions: SessionRegistry;
        coordinatorManager: string;
        coordinatorHuman: string;
        originHuman: string;
        projection: ElasticFleetProjection;
        fetchImpl?: typeof fetch;
    }) { }
    async resolve(worker: ElasticHumanWorker): Promise<EnrolledHumanOrigin> {
        const matches = this.options.store.list().filter(a => a.workerId === worker.workerId);
        const attempt = matches.length === 1 ? matches[0] : null;
        if (!attempt)
            throw Error('enrolled_origin_unavailable');
        const native = await this.options.enrollment.registeredEndpoint(attempt, this.options.configuration());
        const actual: ElasticHumanWorker = { workerId: attempt.workerId, vmId: attempt.binding.vmId, nativeHostId: native.probe.selfHostId, fleetEpoch: attempt.identity.fleetEpoch, manifestDigest: attempt.binding.manifestDigest };
        if (Object.keys(actual).some(k => actual[k as keyof ElasticHumanWorker] !== worker[k as keyof ElasticHumanWorker]))
            throw Error('enrolled_origin_changed');
        this.managers.set(actual.workerId, new Set(native.probe.preparation.seats.filter(s => s.launched && s.nodeId && s.generation).map(s => s.sessionName)));
        return { worker: actual, host: native.host };
    }
    async all(): Promise<EnrolledHumanOrigin[]> {
        const origins: EnrolledHumanOrigin[] = [];
        for (const a of this.options.store.list().filter(a => a.registration)) {
            try {
                origins.push({ worker: { workerId: a.workerId, vmId: a.binding.vmId, nativeHostId: a.registration!.selfHostId, fleetEpoch: a.identity.fleetEpoch, manifestDigest: a.binding.manifestDigest }, host: this.options.enrollment.recordedEndpoint(a) });
            }
            catch { /* Keep durable mappings; unreachable origins are not retired. */ }
        }
        const unique = origins.filter(o => origins.filter(other => other.worker.workerId === o.worker.workerId).length === 1);
        this.current = new Map(unique.map(o => [o.worker.workerId, o]));
        return unique;
    }
    authority(): ElasticHumanProxyAuthority {
        return { currentWorker: id => this.current.get(id)?.worker ?? null, isLocalManager: s => s === this.options.coordinatorManager && !!this.options.sessions.currentOccupantGenerationForSession(s),
            isOriginManager: (worker, s) => this.current.has(worker.workerId) && this.managers.get(worker.workerId)?.has(s) === true, isHumanBinding: (local, origin) => local === this.options.coordinatorHuman && origin === this.options.originHuman };
    }
    /** Registration precedes readiness. Exact native task readback reconciles an uncertain create. */
    async readiness(attemptId: string): Promise<void> {
        let attempt = this.options.store.read(attemptId);
        const plan = this.options.enrollment.preparationPlan();
        const owner = plan.owner;
        if (!owner || owner.sourceSession !== this.options.coordinatorManager || !this.options.sessions.currentOccupantGenerationForSession(owner.sourceSession))
            throw Error('native_readiness_owner_required');
        attempt = await this.options.enrollment.ensureElasticRegistration(attempt, this.options.configuration());
        const origin = await this.resolve({ workerId: attempt.workerId, vmId: attempt.binding.vmId, nativeHostId: attempt.registration!.selfHostId, fleetEpoch: attempt.identity.fleetEpoch, manifestDigest: attempt.binding.manifestDigest });
        for (const task of owner.tasks) {
            const sourceSession = `${owner.sourceSession}@${owner.originHostId}`;
            const input = { qitemId: task.qitemId, sourceSession, destinationSession: task.sessionName, body: readinessTaskBody({ qitemId: task.qitemId, nonce: task.nonce, sourceSession: owner.sourceSession, originHostId: owner.originHostId }, task.sessionName, this.options.enrollment.workerWorkspace(task.sessionName), plan.manifestDigest), tags: ['elastic-readiness'], chainOfRecord: [task.qitemId] };
            const read = () => remoteJsonRequest(origin.host, `/api/queue/${task.qitemId}`, { method: 'GET', timeoutMs: 3000, maxBytes: 1024 * 1024, redirect: 'manual', fetchImpl: this.options.fetchImpl });
            let observed = await read();
            if (!observed.ok && observed.kind === 'http' && observed.status === 404) {
                await remoteJsonRequest(origin.host, '/api/queue/create', { method: 'POST', body: input, headers: { 'X-OpenRig-Session': sourceSession, 'X-OpenRig-Relay': owner.originHostId, 'X-OpenRig-Provenance': 'transport:v1' }, timeoutMs: 5000, maxBytes: 1024 * 1024, redirect: 'manual', fetchImpl: this.options.fetchImpl });
                observed = await read();
            }
            if (!observed.ok)
                throw Error('native_readiness_delivery_unknown');
            const actual = parseNativeHumanQuestion(observed.payload);
            if (actual.qitemId !== input.qitemId || actual.sourceSession !== input.sourceSession || actual.destinationSession !== input.destinationSession || actual.body !== input.body || !actual.chainOfRecord?.includes(input.qitemId))
                throw Error('native_readiness_task_conflict');
        }
    }
    /** Publish full observed capacity; null/busy/drain evidence also resets the idle window. */
    async refresh(maximumGapMs: number): Promise<FleetWorkerObservation[]> {
        const workers: FleetWorkerObservation[] = [];
        for (const a of this.options.store.list()) {
            const observation = await this.options.enrollment.observeManagedWorker(a, this.options.configuration());
            this.options.projection.publish(a, observation);
            const idleSince = this.options.projection.idleSince(a, observation, maximumGapMs);
            workers.push({ ...observation.worker, idleSince });
        }
        await this.all();
        return workers;
    }
    /** Existing GitHub issue reader/manual transports stay in place. Elastic sends resolve the exact
     * enrolled endpoint anew and reject redirects; no arbitrary host registry entry grants admission. */
    intakeIO(base: GithubIntakeIO): GithubIntakeIO {
        const remote = async (host: string, path: string, input?: unknown) => {
            const a = this.options.store.list().find(a => a.registration?.hostId === host);
            if (!a) {
                if (host.startsWith('elastic-'))
                    throw Error('elastic_origin_retired');
                return null;
            }
            const origin = await this.resolve({ workerId: a.workerId, vmId: a.binding.vmId, nativeHostId: a.registration!.selfHostId, fleetEpoch: a.identity.fleetEpoch, manifestDigest: a.binding.manifestDigest });
            return remoteJsonRequest(origin.host, path, { method: input === undefined ? 'GET' : 'POST', body: input, headers: { 'X-OpenRig-Session': 'github-intake@system' }, timeoutMs: 5000, maxBytes: 1024 * 1024, redirect: 'manual', fetchImpl: this.options.fetchImpl });
        };
        const task = (raw: unknown): GithubTask => parseNativeHumanQuestion(raw);
        return { ...base, observeTargets: async config => {
                const managed = await this.all();
                const registry = this.options.enrollment.hostRegistry();
                for (const target of config.targets.filter(t => t.host !== 'local')) {
                    const resolved = resolveHost(registry,target.host);
                    if (!resolved.ok) continue;
                    if (managed.some(o => resolved.host.hostId === o.worker.nativeHostId || resolved.host.transport === 'http' && resolved.host.url === o.host.url)) throw Error('ambiguous_manual_elastic_host_alias');
                }
                return base.observeTargets(config);
            }, getTask: async (host, id) => { const r = await remote(host, `/api/queue/${encodeURIComponent(id)}`); if (!r)
                return base.getTask(host, id); if (!r.ok) {
                if (r.kind === 'http' && r.status === 404)
                    return null;
                throw Error('elastic_task_readback_unknown');
            } const result = task(r.payload); if (result.qitemId !== id)
                throw Error('elastic_task_identity_changed'); return result; },
            createTask: async (host, input) => { const r = await remote(host, '/api/queue/create', input); if (!r)
                return base.createTask(host, input); if (!r.ok)
                throw Error('elastic_task_delivery_unknown'); const result = task(r.payload); if (result.qitemId !== input.qitemId)
                throw Error('elastic_task_identity_changed'); return result; } };
    }
}
