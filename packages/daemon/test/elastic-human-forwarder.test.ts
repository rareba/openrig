import { afterEach, expect, it } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { EventBus } from '../src/domain/event-bus.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { humanResponsesRoutes } from '../src/routes/human-responses.js';
import { elasticWorkerRoutes } from '../src/routes/elastic-worker.js';
import { queueRoutes } from '../src/routes/queue.js';
import { ElasticNativeOwner } from '../src/adapters/elastic-native-owner.js';
import { ElasticHumanForwarder, parseNativeHumanQuestion, type EnrolledHumanOrigin } from '../src/adapters/elastic-human-forwarder.js';
const clean: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const fn of clean.splice(0).reverse())
    await fn(); });
const question = { id: 'go', question: 'Proceed?', options: [{ id: 'yes', label: 'Proceed' }, { id: 'no', label: 'Wait' }] };
const worker = { workerId: 'worker-1', vmId: 'vm-1', nativeHostId: 'native-1', fleetEpoch: 'epoch-1', manifestDigest: 'a'.repeat(64) };
async function fixture() {
    const root = mkdtempSync(join(tmpdir(), 'elastic-forward-'));
    clean.push(() => rmSync(root, { recursive: true, force: true }));
    const db = createDb(join(root, 'coordinator.db')), other = createDb(join(root, 'worker.db'));
    migrate(db, ALL_MIGRATIONS);
    migrate(other, ALL_MIGRATIONS);
    clean.push(() => { db.close(); other.close(); });
    const authority = { currentWorker: () => worker, isLocalManager: (s: string) => s === 'manager@coordinator', isOriginManager: (_w: unknown, s: string) => s === 'manager@worker', isHumanBinding: (l: string, r: string) => l === 'human@kernel' && r === 'operator@external' };
    const coordinator = new QueueRepository(db, new EventBus(db), { validateRig: () => true, elasticHumanProxyAuthority: authority });
    coordinator.attachOutbox(new OutboxHandler(db));
    const origin = new QueueRepository(other, new EventBus(other), { validateRig: () => true });
    origin.attachOutbox(new OutboxHandler(other));
    const app = new Hono();
    app.use('*', async (c, next) => { c.set('queueRepo' as never, origin as never); await next(); });
    app.route('/api/elastic-worker', elasticWorkerRoutes({ bearerToken: 'fixture-bearer', queue: origin }));
    app.route('/api/mission-control', humanResponsesRoutes({ queueRepo: origin, bearerToken: 'fixture-bearer' }));
    app.route('/api/queue', queueRoutes());
    const server = serve({ fetch: app.fetch, port: 0, hostname: '127.0.0.1' });
    await new Promise<void>(r => server.once('listening', r));
    clean.push(() => new Promise<void>((resolve, reject) => server.close(e => e ? reject(e) : resolve())));
    const address = server.address();
    if (!address || typeof address === 'string')
        throw Error();
    const endpoint: EnrolledHumanOrigin = { worker, host: { id: 'native-1', transport: 'http', url: `http://127.0.0.1:${address.port}`, bearer_env: 'ELASTIC_FORWARD_FIXTURE_TOKEN' } };
    let now = 10000, creates = 0, lost = false, malformed = false;
    const requests: string[] = [];
    const fetchImpl: typeof fetch = async (input, init) => {
        const path = new URL(String(input)).pathname;
        requests.push(path);
        expect(init?.redirect).toBe('manual');
        const headers = new Headers(init?.headers);
        expect(headers.get('Authorization')).toBe('Bearer fixture-bearer');
        const response = await fetch(input, init);
        if (path.endsWith('/human-response-receipt') && response.ok && malformed) {
            malformed = false;
            return new Response(JSON.stringify({ ok: true }));
        }
        if (path.endsWith('/human-response')) {
            creates++;
            if (lost) {
                lost = false;
                throw Error('lost acknowledgment after actual native commit');
            }
        }
        return response;
    };
    const directory = { all: async () => [endpoint], resolve: async (w: typeof worker) => { if (JSON.stringify(w) !== JSON.stringify(worker))
            throw Error(); return endpoint; } };
    const make = (fetchOverride: typeof fetch = fetchImpl) => new ElasticHumanForwarder({ db, queue: coordinator, owner: new ElasticNativeOwner(db, join(root, 'coordinator.db')), coordinatorHostId: 'coordinator', coordinatorManager: 'manager@coordinator', coordinatorHuman: 'human@kernel', directory, fetchImpl: fetchOverride, now: () => now, batch: 20 });
    // resolveBearer uses a fixture-only env token; no real credential reader.
    const previous = process.env.ELASTIC_FORWARD_FIXTURE_TOKEN;
    process.env.ELASTIC_FORWARD_FIXTURE_TOKEN = 'fixture-bearer';
    clean.push(() => { if (previous === undefined)
        delete process.env.ELASTIC_FORWARD_FIXTURE_TOKEN;
    else
        process.env.ELASTIC_FORWARD_FIXTURE_TOKEN = previous; });
    const ask = async (id: string, questions = [question], intent: 'decision' | 'update' | null = 'decision') => origin.create({ qitemId: id, sourceSession: 'manager@worker', destinationSession: 'operator@external', body: 'Proceed?', summary: 'Decision', evidenceRef: 'fixture:evidence', humanIntent: intent, humanQuestions: intent === 'update' ? undefined : questions, nudge: false });
    const answer = (qitemId: string, response = { kind: 'choice' as const, questionId: 'go', optionId: 'yes' }, provenance: 'transport:v1' | 'relay:v1' | 'claimed:v1' = 'claimed:v1') => { const proxy = coordinator.getById(qitemId)!; return coordinator.recordHumanResponse({ qitemId, replyId: 'answer-' + qitemId, requestRevision: proxy.humanRequestRevision!, actorSession: 'human@kernel', identityProvenance: provenance, response }); };
    return { db, other, coordinator, origin, ask, answer, make, requests, endpoint, loseAck: () => lost = true, malformReceipt: () => malformed = true, creates: () => creates, advance: () => now += 100000 };
}
it('discovers native null-intent questions through HTTP and applies one exact answer after a lost acknowledgment with no coordinator wake', async () => {
    const f = await fixture();
    await f.ask('qitem-original', [question], null);
    await f.ask('qitem-update', [question], 'update');
    expect(() => parseNativeHumanQuestion(f.origin.getById('qitem-original'))).not.toThrow();
    const projection = await fetch(f.endpoint.host.url + '/api/elastic-worker/human-questions?after=0&limit=20', { headers: { Authorization: 'Bearer fixture-bearer' } });
    expect(await projection.json()).toMatchObject({ items: [{ qitemId: 'qitem-original' }] });
    const forwarder = f.make();
    expect((await forwarder.run()).discovered).toBe(1);
    const proxy = f.coordinator.listAttention()[0]!;
    const receipt = f.answer(proxy.qitemId, undefined, 'transport:v1');
    const [intent] = f.coordinator.pendingElasticHumanForwards();
    expect(intent!.originInput.identityProvenance).toBe('relay:v1');
    f.loseAck();
    expect((await forwarder.run()).settled).toBe(1);
    expect(f.origin.getById('qitem-original')?.state).toBe('done');
    expect(f.creates()).toBe(1);
    expect(f.coordinator.getById(receipt.replyQitemId)?.state).toBe('done');
    await f.make().run();
    expect(f.creates()).toBe(1);
    expect(f.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 0 });
    expect(f.other.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 1 });
    const continuation = f.origin.getById(f.origin.humanResponseCommitEvidence(intent!.originInput).receipt.replyQitemId)!;
    f.origin.claim({ qitemId: continuation.qitemId, destinationSession: 'manager@worker' });
    expect(f.origin.getById(continuation.qitemId)?.claimedAt).toBeTruthy();
});
it('rotates beyond the first100 native questions and isolates an invalid first forwarding authority', async () => {
    const f = await fixture();
    for (let i = 0; i < 105; i++)
        await f.ask(`qitem-question-${i}`);
    for (let i = 0; i < 6; i++)
        await f.make().run();
    expect(f.coordinator.listAttention({ limit: 200 })).toHaveLength(105);
    const all = f.coordinator.listAttention({ limit: 200 });
    for (const q of all)
        f.answer(q.qitemId);
    // One wrong current authority must not throw the entire typed enumeration or starve later tuples.
    const original = f.coordinator.scanElasticHumanForwards(0, 1).items[0]!;
    expect('intent' in original).toBe(true);
    if ('intent' in original)
        f.db.prepare("UPDATE elastic_human_proxies SET mapping=json_set(mapping,'$.worker.vmId','replaced') WHERE intent_id=?").run(original.intent.intentId);
    for (let i = 0; i < 6; i++)
        await f.make().run();
    expect(f.origin.getById('qitem-question-104')?.state).toBe('done');
    expect(f.creates()).toBe(104);
    expect(f.coordinator.scanElasticHumanForwards(0, 100).items).toHaveLength(1);
});
it('keeps partial and clarification sequences pending until native readback changes; retains a direct winner conflict', async () => {
    const f = await fixture();
    await f.ask('qitem-partial', [question, { ...question, id: 'when', question: 'When?' }]);
    await f.make().run();
    const proxy = f.coordinator.listAttention()[0]!;
    f.answer(proxy.qitemId);
    await f.make().run();
    expect(f.coordinator.listAttention()).toHaveLength(1);
    expect(f.coordinator.listAttention()[0]!.humanQuestions?.map(q => q.id)).toEqual(['when']);
    // A direct origin winner changes the pending tuple before its mapped response can apply.
    const next = f.coordinator.getById(f.coordinator.listAttention()[0]!.qitemId)!;
    f.coordinator.recordHumanResponse({ qitemId: next.qitemId, replyId: 'mapped-second', requestRevision: next.humanRequestRevision!, actorSession: 'human@kernel', response: { kind: 'choice', questionId: 'when', optionId: 'yes' } });
    const original = f.origin.getById('qitem-partial')!;
    f.origin.recordHumanResponse({ qitemId: original.qitemId, replyId: 'direct', requestRevision: original.humanRequestRevision!, actorSession: 'operator@external', response: { kind: 'choice', questionId: 'when', optionId: 'no' } });
    await f.make().run();
    expect(f.coordinator.getById(next.qitemId)?.humanForwardingStatus).toBe('conflict');
    expect(f.coordinator.scanElasticHumanForwards(0).items).toHaveLength(0);
    expect(f.db.prepare("SELECT state FROM elastic_human_proxies WHERE proxy_qitem_id=?").get(next.qitemId)).toEqual({ state: 'conflict' });
    expect(f.db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id LIKE 'elastic-human-forward-%' ORDER BY rowid DESC LIMIT 1").get()).toEqual({ delivery_state: 'pending' });
});
it('retains a malformed successful commit after native application and reconciles the exact immutable intent after consumer restart', async () => {
    const f = await fixture();
    await f.ask('qitem-malformed');
    await f.make().run();
    const proxy = f.coordinator.listAttention()[0]!;
    f.answer(proxy.qitemId);
    const [saved] = f.coordinator.pendingElasticHumanForwards();
    f.origin.recordHumanResponse(saved!.originInput);
    f.malformReceipt();
    expect((await f.make().run()).settled).toBe(0);
    expect(f.coordinator.pendingElasticHumanForwards()).toEqual([saved]);
    f.advance();
    expect((await f.make().run()).settled).toBe(1);
    expect(f.creates()).toBe(0);
});
it('does not repeat an unchanged clarification and creates one new sequence after the real origin manager changes the question', async () => {
    const f = await fixture();
    await f.ask('qitem-clarification');
    await f.make().run();
    const proxy = f.coordinator.getById(f.coordinator.listAttention()[0]!.qitemId)!;
    f.coordinator.recordHumanResponse({ qitemId: proxy.qitemId, replyId: 'clarification', requestRevision: proxy.humanRequestRevision!, actorSession: 'human@kernel', response: { kind: 'clarification', text: 'Why?' } });
    await f.make().run();
    await f.make().run();
    expect(f.coordinator.getById(proxy.qitemId)?.humanForwardingStatus).toBe('awaiting-origin');
    expect(f.coordinator.pendingElasticHumanForwards()).toEqual([]);
    expect(f.db.prepare('SELECT state FROM elastic_human_proxies').get()).toEqual({ state: 'awaiting-origin' });
    f.origin.update({ qitemId: 'qitem-clarification', actorSession: 'manager@worker', summary: 'Proceed only in synthetic test' });
    await f.make().run();
    await f.make().run();
    expect(f.coordinator.listAttention()).toHaveLength(1);
    expect(f.db.prepare('SELECT sequence FROM elastic_human_proxies ORDER BY sequence').all()).toEqual([{ sequence: 0 }, { sequence: 1 }]);
});
it('suppresses an existing open proxy when the origin is answered directly before any local answer', async () => {
    const f = await fixture(); await f.ask('qitem-direct-first'); await f.make().run();
    const proxy = f.coordinator.getById(f.coordinator.listAttention()[0]!.qitemId)!;
    const origin = f.origin.getById('qitem-direct-first')!;
    f.origin.recordHumanResponse({ qitemId: origin.qitemId, replyId: 'direct-first', actorSession: 'operator@external', requestRevision: origin.humanRequestRevision!, response: { kind: 'choice', questionId: 'go', optionId: 'yes' } });
    await f.make().run();
    expect(f.coordinator.getById(proxy.qitemId)?.humanForwardingStatus).toBe('conflict');
    expect(() => f.answer(proxy.qitemId)).toThrow();
    expect(f.creates()).toBe(0);
    expect(f.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'elastic-human-forward-%'").get()).toEqual({ n: 0 });
});
it('revisits an awaiting clarification after its origin becomes terminal without inventing another forwarded receipt', async () => {
    const f = await fixture(); await f.ask('qitem-terminal-clarification'); await f.make().run();
    const proxy = f.coordinator.getById(f.coordinator.listAttention()[0]!.qitemId)!;
    f.coordinator.recordHumanResponse({ qitemId: proxy.qitemId, replyId: 'clarify-terminal', requestRevision: proxy.humanRequestRevision!, actorSession: 'human@kernel', response: { kind: 'clarification', text: 'Why?' } });
    await f.make().run();
    const origin = f.origin.getById('qitem-terminal-clarification')!;
    f.origin.recordHumanResponse({ qitemId: origin.qitemId, replyId: 'direct-terminal', requestRevision: origin.humanRequestRevision!, actorSession: 'operator@external', response: { kind: 'choice', questionId: 'go', optionId: 'no' } });
    await f.make().run();
    expect(f.coordinator.getById(proxy.qitemId)?.humanForwardingStatus).toBe('conflict');
    expect(f.db.prepare('SELECT sequence,state FROM elastic_human_proxies').all()).toEqual([{ sequence: 0, state: 'conflict' }]);
    expect(f.creates()).toBe(1);
    expect(f.db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id LIKE 'elastic-human-forward-%'").all()).toEqual([{ delivery_state: 'delivered' }]);
});
it('pages existing terminal-origin mappings past100 while an unavailable first origin retains its own custody', async () => {
    const f = await fixture();
    for (let i = 0; i < 105; i++) await f.ask(`qitem-terminal-page-${i}`);
    for (let i = 0; i < 6; i++) await f.make().run();
    const proxies = f.coordinator.listAttention({ limit: 200 });
    for (let i = 0; i < 105; i++) {
        const origin = f.origin.getById(`qitem-terminal-page-${i}`)!;
        f.origin.recordHumanResponse({ qitemId: origin.qitemId, replyId: `direct-${i}`, requestRevision: origin.humanRequestRevision!, actorSession: 'operator@external', response: { kind: 'choice', questionId: 'go', optionId: 'yes' } });
    }
    // Native readback of the first mapping is unavailable; all later readbacks remain real HTTP.
    const base = f.endpoint.host.url;
    const fetchImpl: typeof fetch = async (input, init) => String(input) === `${base}/api/queue/qitem-terminal-page-0` ? new Response('{}', { status: 503 }) : fetch(input, init);
    for (let i = 0; i < 6; i++) await f.make(fetchImpl).run();
    const mappings = f.db.prepare('SELECT state,count(*) n FROM elastic_human_proxies GROUP BY state').all();
    expect(mappings).toEqual([{ state: 'conflict', n: 104 }, { state: 'open', n: 1 }]);
    expect(f.creates()).toBe(0); expect(proxies).toHaveLength(105);
});
