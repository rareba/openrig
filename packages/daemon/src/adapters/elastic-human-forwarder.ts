import type { ElasticEnrolledWorkerDirectory } from './elastic-coordinator-admission.js';
import { CLOSURE_REASONS } from '../domain/hot-potato-enforcer.js';
import type Database from 'better-sqlite3';
import { z } from 'zod';
import { createHash } from 'node:crypto';
import { parseHumanResponse, parseHumanQuestions } from '../domain/human-questions.js';
import type { ElasticHumanWorker, NativeHumanResponseCommit } from '../domain/elastic-human-proxy.js';
import type { QueueItem, QueueRepository } from '../domain/queue-repository.js';
import type { HttpHostEntry } from '../domain/hosts/hosts-registry-reader.js';
import { remoteJsonRequest } from '../domain/hosts/remote-daemon-http.js';
import { ElasticNativeOwner } from './elastic-native-owner.js';
const questionSchema = z.object({ qitemId: z.string().regex(/^[A-Za-z0-9_-]{1,128}$/), sourceSession: z.string(), destinationSession: z.string(), state: z.enum(['pending', 'in-progress', 'blocked', 'done', 'canceled', 'denied', 'failed', 'handed-off']), humanRequestRevision: z.string().optional(), humanIntent: z.enum(['decision', 'update']).nullable().optional(), body: z.string(), summary: z.string().nullable(), evidenceRef: z.string().nullable(), humanQuestions: z.unknown().optional(), humanAnswers: z.record(z.string(), z.union([z.string(), z.object({ kind: z.literal('typed-reply'), text: z.string(), placement: z.literal('first-unanswered'), unansweredCount: z.number().int().nonnegative() }).strict()])).nullable().optional(), tsCreated: z.string(), tsUpdated: z.string(), tags: z.array(z.string()).nullable(), blockedOn: z.string().nullable(), claimedAt: z.string().nullable(), priority: z.enum(['routine', 'urgent', 'critical']), tier: z.string().nullable(), handedOffTo: z.string().nullable(), handedOffFrom: z.string().nullable(), expiresAt: z.string().nullable(), chainOfRecord: z.array(z.string()).nullable(), closureReason: z.enum(CLOSURE_REASONS).nullable(), closureTarget: z.string().nullable(), closureRequiredAt: z.string().nullable(), lastNudgeAttempt: z.string().nullable(), lastNudgeResult: z.string().nullable(), lastNudgeWireResult: z.string().nullable(), lastHeartbeat: z.string().nullable(), resolution: z.string().nullable(), targetRepo: z.string().nullable() }).passthrough();
export function parseNativeHumanQuestion(raw: unknown): QueueItem {
    const value = questionSchema.parse(raw);
    const questions = value.humanQuestions == null ? null : parseHumanQuestions(value.humanQuestions);
    if (questions && !questions.ok)
        throw Error('native_question_invalid');
    return { ...value, humanQuestions: questions?.ok ? questions.questions : null };
}
function parseCommit(raw: unknown): NativeHumanResponseCommit {
    const value = z.object({ qitemId: z.string(), actorSession: z.string(), requestRevision: z.string(), response: z.unknown(), fingerprint: z.string().regex(/^[a-f0-9]{64}$/), receipt: z.object({ replyQitemId: z.string(), complete: z.boolean(), replyHostId: z.string().optional() }).strict() }).strict().parse(raw);
    const response = parseHumanResponse(value.response);
    if (!response)
        throw Error('native_commit_invalid');
    return { ...value, response };
}
export interface EnrolledHumanOrigin {
    worker: ElasticHumanWorker;
    host: HttpHostEntry;
}
/** trustedOrigins must come from the native enrollment adapter, never question text/HTTP input. */
export class ElasticHumanForwarder {
    constructor(private readonly options: {
        db: Database.Database;
        queue: QueueRepository;
        owner: ElasticNativeOwner;
        coordinatorHostId: string;
        coordinatorManager: string;
        coordinatorHuman: string;
        directory: Pick<ElasticEnrolledWorkerDirectory, 'all' | 'resolve'>;
        fetchImpl?: typeof fetch;
        now?: () => number;
        batch?: number;
    }) { }
    private now() { return (this.options.now ?? Date.now)(); }
    private cursor(key: string): number { return (this.options.db.prepare('SELECT cursor FROM elastic_consumer_progress WHERE consumer=?').get(key) as {
        cursor: number;
    } | undefined)?.cursor ?? 0; }
    private progress(key: string, cursor: number) { this.options.db.prepare('INSERT INTO elastic_consumer_progress VALUES(?,?) ON CONFLICT(consumer) DO UPDATE SET cursor=excluded.cursor').run(key, cursor); }
    private request(origin: EnrolledHumanOrigin, path: string, body?: unknown, headers?: Record<string, string>) {
        const request = async () => { const current = await this.options.directory.resolve(origin.worker); return remoteJsonRequest(current.host, path, { method: body === undefined ? 'GET' : 'POST', body, headers, timeoutMs: 3000, maxBytes: 2 * 1024 * 1024, redirect: 'manual', fetchImpl: this.options.fetchImpl }); };
        return request();
    }
    async run(): Promise<{
        discovered: number;
        settled: number;
        retained: number;
    }> { return this.options.owner.run('human-forwarding', () => this.runOwned()); }
    private async runOwned() {
        const stats = { discovered: 0, settled: 0, retained: 0 };
        const batch = this.options.batch ?? 20;
        if (!Number.isInteger(batch) || batch < 1 || batch > 100)
            throw Error('invalid_forward_batch');
        const candidates = await this.options.directory.all();
        const origins = candidates.filter(o => candidates.filter(other => other.worker.workerId === o.worker.workerId).length === 1).sort((a, b) => a.worker.workerId.localeCompare(b.worker.workerId));
        stats.retained += candidates.length - origins.length;
        const start = origins.length ? this.cursor('origin-rotation') % origins.length : 0;
        // One bounded page per origin, bounded origins per turn, rotation survives restart.
        for (let i = 0; i < Math.min(batch, origins.length); i++) {
            const origin = origins[(start + i) % origins.length]!;
            const key = `questions:${origin.worker.workerId}:${origin.worker.vmId}`;
            try {
                const page = await this.request(origin, `/api/elastic-worker/human-questions?after=${this.cursor(key)}&limit=${batch}${this.cursor(key + ':through') ? `&through=${this.cursor(key + ':through')}` : ''}`);
                if (!page.ok)
                    throw Error();
                const p = z.object({ items: z.array(z.unknown()).max(batch), next: z.number().int().nonnegative(), through: z.number().int().nonnegative(), complete: z.boolean() }).strict().parse(page.payload);
                if (p.next < this.cursor(key) || p.next > p.through || p.complete && p.next !== p.through || !p.complete && p.next === this.cursor(key))
                    throw Error();
                for (const raw of p.items) {
                    try {
                        const candidate = parseNativeHumanQuestion(raw);
                        const read = await this.request(origin, `/api/queue/${encodeURIComponent(candidate.qitemId)}`);
                        if (!read.ok)
                            throw Error();
                        const question = parseNativeHumanQuestion(read.payload);
                        if (question.qitemId !== candidate.qitemId)
                            throw Error();
                        this.options.queue.reconcileDiscoveredElasticHuman({ worker: origin.worker, origin: question, coordinatorManager: this.options.coordinatorManager, coordinatorHuman: this.options.coordinatorHuman });
                        stats.discovered++;
                    }
                    catch {
                        stats.retained++;
                    }
                }
                this.options.db.transaction(() => { this.progress(key, p.complete ? 0 : p.next); this.progress(key + ':through', p.complete ? 0 : p.through); })();
            }
            catch {
                stats.retained++;
            }
        }
        if (origins.length)
            this.progress('origin-rotation', (start + Math.min(batch, origins.length)) % origins.length);
        const mappings = this.options.queue.scanElasticHumanMappings(this.cursor('mapping-readbacks'), batch);
        for (const row of mappings.items) {
            if ('error' in row) { stats.retained++; continue; }
            const mapping = row.mapping;
            try {
                const origin = origins.find(o => o.worker.workerId === mapping.worker.workerId && o.worker.vmId === mapping.worker.vmId && o.worker.nativeHostId === mapping.worker.nativeHostId && o.worker.fleetEpoch === mapping.worker.fleetEpoch && o.worker.manifestDigest === mapping.worker.manifestDigest);
                if (!origin) throw Error('mapped_origin_unavailable');
                const read = await this.request(origin, `/api/queue/${encodeURIComponent(mapping.originQitemId)}`);
                if (!read.ok) throw Error('mapped_origin_readback_unavailable');
                const question = parseNativeHumanQuestion(read.payload);
                if (question.qitemId !== mapping.originQitemId) throw Error('mapped_origin_identity_changed');
                if (question.state !== 'pending') this.options.queue.reconcileElasticHumanProxy({ proxyQitemId: mapping.proxyQitemId, worker: origin.worker, origin: question, disposition: 'origin-changed' });
                else this.options.queue.reconcileDiscoveredElasticHuman({ worker: origin.worker, origin: question, coordinatorManager: mapping.coordinatorManager, coordinatorHuman: mapping.coordinatorHuman });
            } catch { stats.retained++; }
        }
        this.progress('mapping-readbacks', mappings.complete ? 0 : mappings.next);
        const page = this.options.queue.scanElasticHumanForwards(this.cursor('forwards'), batch);
        for (const row of page.items) {
            if ('error' in row) {
                stats.retained++;
                continue;
            }
            const intent = row.intent;
            const prior = this.options.db.prepare('SELECT attempted,next_at,failures FROM elastic_forward_attempts WHERE intent_id=?').get(intent.intentId) as {
                attempted: number;
                next_at: number;
                failures: number;
            } | undefined;
            if (prior && prior.next_at > this.now())
                continue;
            try {
                const origin = origins.find(o => JSON.stringify(o.worker) === JSON.stringify(intent.mapping.worker));
                if (!origin)
                    throw Error();
                const headers = { 'X-OpenRig-Session': intent.originInput.actorSession, 'X-OpenRig-Relay': this.options.coordinatorHostId, 'X-OpenRig-Provenance': intent.originInput.identityProvenance ?? 'claimed:v1' };
                // Publish before send. After crash/unknown, inspect exact committed tuple first.
                this.options.db.prepare('INSERT INTO elastic_forward_attempts(intent_id,attempted) VALUES(?,1) ON CONFLICT(intent_id) DO UPDATE SET attempted=1').run(intent.intentId);
                let committed = await this.request(origin, '/api/mission-control/human-response-receipt', intent.originInput, headers);
                if (!committed.ok && committed.kind === 'http' && committed.status === 409 && committed.detail === 'human_response_not_committed') {
                    await this.request(origin, '/api/mission-control/human-response', intent.originInput, headers);
                    committed = await this.request(origin, '/api/mission-control/human-response-receipt', intent.originInput, headers);
                }
                if (!committed.ok) {
                    if (committed.kind === 'http' && committed.status === 409) {
                        const read = await this.request(origin, `/api/queue/${encodeURIComponent(intent.mapping.originQitemId)}`);
                        if (read.ok) {
                            const current = parseNativeHumanQuestion(read.payload);
                            if (current.qitemId === intent.mapping.originQitemId && (current.state !== 'pending' || current.humanRequestRevision !== intent.mapping.originRevision))
                                this.options.queue.reconcileElasticHumanProxy({ proxyQitemId: intent.mapping.proxyQitemId, worker: origin.worker, origin: current, disposition: 'origin-changed' });
                        }
                    }
                    throw Error();
                }
                const commit = parseCommit(committed.payload);
                if (commit.fingerprint !== createHash('sha256').update(JSON.stringify([intent.originInput.requestRevision, intent.originInput.response])).digest('hex'))
                    throw Error();
                const read = await this.request(origin, `/api/queue/${encodeURIComponent(intent.mapping.originQitemId)}`);
                if (!read.ok)
                    throw Error();
                const question = parseNativeHumanQuestion(read.payload);
                this.options.queue.settleElasticHumanForward({ intentId: intent.intentId, worker: origin.worker, commit, origin: question });
                if (!commit.receipt.complete)
                    this.options.queue.reconcileDiscoveredElasticHuman({ worker: origin.worker, origin: question, coordinatorManager: this.options.coordinatorManager, coordinatorHuman: this.options.coordinatorHuman });
                this.options.db.prepare('DELETE FROM elastic_forward_attempts WHERE intent_id=?').run(intent.intentId);
                stats.settled++;
            }
            catch {
                const failures = Math.min(16, (prior?.failures ?? 0) + 1);
                this.options.db.prepare('INSERT INTO elastic_forward_attempts VALUES(?,1,?,?,?) ON CONFLICT(intent_id) DO UPDATE SET next_at=excluded.next_at,failures=excluded.failures,reason=excluded.reason').run(intent.intentId, this.now() + Math.min(60000, 1000 * 2 ** failures), failures, 'origin_receipt_or_authority_unavailable');
                stats.retained++;
            }
        }
        this.progress('forwards', page.complete ? 0 : page.next);
        return stats;
    }
}
