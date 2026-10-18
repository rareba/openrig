import type Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { enrollmentDigest, type ElasticWorkerAttempt } from '../adapters/elastic-worker-attempts.js';
import type { ManagedWorkerObservation } from '../adapters/elastic-worker-enrollment.js';
import type { GithubIntakeTarget, GithubTargetObservation, GithubIssue, GithubIntakeAdmission } from './github-intake.js';
const projectionSchema = z.object({ host: z.string(), session: z.string(), physicalHost: z.string(), capacity: z.literal(1), capabilities: z.array(z.string()), workerId: z.string(), vmId: z.string(), fleetEpoch: z.string(), manifestDigest: z.string(), fleetRevision: z.number().int(), generation: z.number().int(), observedAt: z.number().int(), expiresAt: z.number().int().nullable(), available: z.boolean(), active: z.number().int().nonnegative().nullable(), hostActive: z.number().int().nonnegative().nullable(), repositories: z.array(z.object({ id: z.string(), publicRead: z.boolean(), privateRead: z.boolean(), write: z.boolean() }).strict()) }).strict();
type Projection = z.infer<typeof projectionSchema>;
/** Nonsecret eligibility only. Private enrollment/credential files never enter SQL. */
export class ElasticFleetProjection implements GithubIntakeAdmission {
    private readonly observer = randomUUID();
    constructor(private readonly db: Database.Database, private readonly now = Date.now) { }
    private fleet() { return this.db.prepare('SELECT revision,paused,config FROM elastic_fleet_config WHERE id=1').get() as {
        revision: number;
        paused: number;
        config: string | null;
    }; }
    publish(attempt: ElasticWorkerAttempt, observation: ManagedWorkerObservation): void {
        this.db.transaction(() => {
            const fleet = this.fleet();
            const probe = observation.probe;
            const capacity = probe?.preparation.capacity;
            const allowed = !!attempt.registration && observation.worker.id === attempt.workerId && probe?.state.workerId === attempt.workerId && probe.state.vmId === attempt.binding.vmId && probe.state.manifestDigest === attempt.binding.manifestDigest && probe.selfHostId === attempt.registration.selfHostId && probe.state.phase === 'ready' && observation.worker.state === 'ready' && !!capacity?.complete;
            const rows = probe?.preparation.seats ?? [];
            const existing = this.db.prepare('SELECT session FROM elastic_target_projection WHERE worker_id=?').all(attempt.workerId) as {
                session: string;
            }[];
            // Missing observations revoke old rows, they never leave stale readiness behind.
            for (const row of existing)
                this.db.prepare("UPDATE elastic_target_projection SET revision=?,projection=json_set(projection,'$.available',json('false')) WHERE worker_id=? AND session=?").run(randomUUID(), attempt.workerId, row.session);
            if (!attempt.registration)
                return;
            for (const seat of rows) {
                const p: Projection = { host: attempt.registration.hostId, session: seat.sessionName, physicalHost: attempt.workerId, capacity: 1, capabilities: observation.worker.capabilities.filter(c => c.startsWith(`github:${seat.repositoryId}:`)),
                    workerId: attempt.workerId, vmId: attempt.binding.vmId, fleetEpoch: attempt.identity.fleetEpoch, manifestDigest: attempt.binding.manifestDigest, fleetRevision: fleet.revision, generation: probe!.state.generation,
                    observedAt: probe!.observedAt, expiresAt: attempt.credential?.expiresAt ?? null, available: allowed && seat.active !== null,
                    active: seat.active === null ? null : Number(seat.active), hostActive: observation.worker.active,
                    repositories: probe!.preparation.repositoryCapabilities.filter(r => r.repositoryId === seat.repositoryId).map(r => ({ id: String(r.repositoryId), publicRead: r.publicRead, privateRead: r.privateRead, write: r.write })) };
                // Same observation is stable; newer samples revoke previous reservation authorization.
                const revision = enrollmentDigest(p);
                const old = this.db.prepare('SELECT worker_id FROM elastic_target_projection WHERE host=? AND session=?').get(p.host, p.session) as {
                    worker_id: string;
                } | undefined;
                if (old && old.worker_id !== p.workerId)
                    throw Error('elastic_target_identity_conflict');
                this.db.prepare('INSERT INTO elastic_target_projection VALUES(?,?,?,?,?) ON CONFLICT(host,session) DO UPDATE SET revision=excluded.revision,projection=excluded.projection').run(p.host, p.session, p.workerId, revision, JSON.stringify(p));
            }
        }).immediate();
    }
    retire(attempt: ElasticWorkerAttempt): void {
        this.db.transaction(() => {
            this.db.prepare('DELETE FROM elastic_target_projection WHERE worker_id=?').run(attempt.workerId);
            this.db.prepare('DELETE FROM elastic_idle_windows WHERE worker_id=?').run(attempt.workerId);
        }).immediate();
    }
    private rows() { return (this.db.prepare('SELECT revision,projection FROM elastic_target_projection ORDER BY host,session').all() as {
        revision: string;
        projection: string;
    }[]).map(r => ({ revision: r.revision, value: projectionSchema.parse(JSON.parse(r.projection)) })); }
    targets(): GithubIntakeTarget[] { return this.rows().map(({ value: p }) => ({ host: p.host, session: p.session, physicalHost: p.physicalHost, capacity: p.capacity, capabilities: p.capabilities })); }
    observations(): GithubTargetObservation[] { return this.rows().map(({ value: p }) => ({ host: p.host, session: p.session, available: this.current(p), active: p.active ?? 0, hostActive: p.hostActive })); }
    private current(p: Projection): boolean {
        const f = this.fleet();
        const config = f.config ? JSON.parse(f.config) : null;
        return p.available && p.active !== null && p.hostActive !== null && !f.paused && !!config?.policy?.enabled && config.policy.mode === 'active' && f.revision === p.fleetRevision
            && p.observedAt <= this.now() && this.now() - p.observedAt <= 5000 && (p.expiresAt === null || p.expiresAt > this.now())
            && !this.db.prepare("SELECT 1 FROM elastic_fleet_drains WHERE worker_id=? AND state<>'removed'").get(p.workerId);
    }
    /** Called synchronously inside intake's owning reservation/delivery transaction. */
    authorize(target: GithubIntakeTarget, issue: GithubIssue, capability: string | null, qitemId: string, phase: 'reserve' | 'delivery' | 'retry'): void {
        if (!this.db.inTransaction)
            throw Error('elastic_admission_transaction_required');
        const row = this.rows().find(r => r.value.host === target.host && r.value.session === target.session);
        const reservation = this.db.prepare('SELECT host,session,revision,binding FROM elastic_intake_reservations WHERE qitem_id=?').get(qitemId) as {
            host: string;
            session: string;
            revision: string;
            binding: string;
        } | undefined;
        if (!row) {
            if (reservation)
                throw Error('elastic_target_retired');
            return;
        }
        const p = row.value;
        const repo = p.repositories.find(r => r.id === issue.repositoryId || issue.repositoryAliases?.includes(r.id));
        if (!this.current(p) || !repo || !capability || !p.capabilities.includes(capability) || ![repo.publicRead && `github:${repo.id}:public-read`, repo.privateRead && `github:${repo.id}:private-read`, repo.write && `github:${repo.id}:write`].includes(capability))
            throw Error('elastic_target_ineligible');
        if (phase !== 'delivery' && p.active !== 0) throw Error('elastic_target_occupied');
        if (phase === 'reserve' && this.db.prepare(`SELECT 1 FROM elastic_intake_reservations r JOIN github_intake_issues i ON i.qitem_id=r.qitem_id
          WHERE r.host=? AND r.session=? AND r.qitem_id<>? AND COALESCE(i.pickup,'unknown') NOT IN ('terminal','waiting-human')
          AND COALESCE(i.current_host,i.target_host)=r.host AND COALESCE(i.current_session,i.target_session)=r.session LIMIT 1`).get(p.host,p.session,qitemId)) throw Error('elastic_target_reserved');
        const binding = JSON.stringify([p.workerId, p.vmId, p.fleetEpoch, p.manifestDigest, p.generation, p.fleetRevision, issue.repositoryId, issue.updatedAt, capability]);
        if (reservation) {
            if (reservation.host !== p.host || reservation.session !== p.session || reservation.binding !== binding)
                throw Error('elastic_reservation_identity_changed');
            if (phase === 'delivery' && reservation.revision !== row.revision)
                throw Error('elastic_eligibility_revision_changed');
            if (phase === 'retry')
                this.db.prepare('UPDATE elastic_intake_reservations SET revision=? WHERE qitem_id=?').run(row.revision, qitemId);
        }
        else {
            if (phase !== 'reserve')
                throw Error('elastic_reservation_missing');
            this.db.prepare('INSERT INTO elastic_intake_reservations VALUES(?,?,?,?,?)').run(qitemId, p.host, p.session, row.revision, binding);
        }
    }
    /** Observation continuity is controller-local and resets after every consumer restart. */
    idleSince(attempt: ElasticWorkerAttempt, observation: ManagedWorkerObservation, maximumGapMs: number): number | null {
        const p = observation.probe;
        const now = this.now();
        const eligible = !!p && observation.worker.state === 'ready' && observation.worker.active === 0 && observation.drainEligible === true && p.ledger.complete && p.ledger.clean && p.process.complete && p.process.idle && p.filesystem.complete && !p.filesystem.blockers.length && p.observedAt <= now && now - p.observedAt <= 5000;
        return this.db.transaction(() => {
            const previous = this.db.prepare('SELECT * FROM elastic_idle_windows WHERE worker_id=?').get(attempt.workerId) as {
                identity: string;
                custody: string;
                owner: string;
                first_at: number;
                last_at: number;
            } | undefined;
            if (!eligible || !Number.isSafeInteger(maximumGapMs) || maximumGapMs <= 0) {
                this.db.prepare('DELETE FROM elastic_idle_windows WHERE worker_id=?').run(attempt.workerId);
                return null;
            }
            const identity = enrollmentDigest([attempt.binding.vmId, attempt.identity.fleetEpoch, p!.state.generation]);
            const custody = observation.idleCustodyDigest ?? enrollmentDigest([p!.ledger.digest, p!.filesystem.digest]);
            const at = p!.observedAt;
            const continuous = previous?.owner === this.observer && previous.identity === identity && previous.custody === custody && at >= previous.last_at && at - previous.last_at <= maximumGapMs && now - previous.last_at <= maximumGapMs;
            if (continuous && at === previous.last_at && now > at)
                return null;
            const first = continuous ? previous.first_at : now;
            this.db.prepare('INSERT INTO elastic_idle_windows VALUES(?,?,?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET identity=excluded.identity,custody=excluded.custody,owner=excluded.owner,first_at=excluded.first_at,last_at=excluded.last_at').run(attempt.workerId, identity, custody, this.observer, first, at);
            return first;
        }).immediate();
    }
}
