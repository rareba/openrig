import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Hono } from 'hono';
import { humanResponsesRoutes } from '../src/routes/human-responses.js';
import { HumanRelayService } from '../src/domain/human-relay.js';
import type { ElasticHumanProxyAuthority, ElasticHumanForward } from '../src/domain/elastic-human-proxy.js';
import { afterEach, describe, expect, it } from 'vitest';
import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { EventBus } from '../src/domain/event-bus.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { QueueRepository } from '../src/domain/queue-repository.js';

const question = { id: 'go', question: 'Proceed?', options: [{ id: 'yes', label: 'Proceed' }, { id: 'no', label: 'Wait' }] };
const worker = { workerId: 'worker-1', vmId: 'vm-1', nativeHostId: 'elastic-1', fleetEpoch: 'epoch-1', manifestDigest: 'a'.repeat(64) };
const databases: ReturnType<typeof createDb>[] = [];
const authority: ElasticHumanProxyAuthority = { currentWorker: () => worker, isLocalManager: s => s === 'manager@coordinator', isOriginManager: (_w, s) => s === 'manager@worker',
  isHumanBinding: (local, origin) => local === 'human@kernel' && origin === 'operator@external' };
function repository(db: ReturnType<typeof createDb>, trusted: ElasticHumanProxyAuthority | undefined = authority) {
  const repo = new QueueRepository(db, new EventBus(db), { validateRig: () => true, elasticHumanProxyAuthority: trusted });
  repo.attachOutbox(new OutboxHandler(db)); return repo;
}
function fixture(path?: string) {
  const db = createDb(path); databases.push(db); migrate(db, ALL_MIGRATIONS);
  return { db, repo: repository(db) };
}
afterEach(() => { for (const db of databases.splice(0)) db.close(); });
async function pair(questions = [question]) {
  const origin = fixture(), coordinator = fixture();
  const asked = await origin.repo.create({ qitemId: 'qitem-original', sourceSession: 'manager@worker', destinationSession: 'operator@external', body: 'Proceed?',
    summary: 'Decision', evidenceRef: 'fixture:evidence', humanIntent: 'decision', humanQuestions: questions, nudge: false });
  const proxy = coordinator.repo.createElasticHumanProxy({ worker, origin: origin.repo.getById(asked.qitemId)!, coordinatorManager: 'manager@coordinator', coordinatorHuman: 'human@kernel' });
  return { origin, coordinator, asked, proxy };
}
function answer(proxy: ReturnType<QueueRepository['getById']> & {}, replyId = 'answer-1') {
  return { qitemId: proxy.qitemId, requestRevision: proxy.humanRequestRevision!, actorSession: 'human@kernel', replyId,
    identityProvenance: 'claimed:v1' as const, response: { kind: 'choice' as const, questionId: 'go', optionId: 'yes' } };
}
function settle(origin: ReturnType<typeof fixture>, coordinator: ReturnType<typeof fixture>, intent: ElasticHumanForward) {
  const receipt = origin.repo.recordHumanResponse(intent.originInput);
  const committed = { intentId: intent.intentId, worker, commit: origin.repo.humanResponseCommitEvidence(intent.originInput), origin: origin.repo.getById(intent.mapping.originQitemId)! };
  coordinator.repo.settleElasticHumanForward(committed); return { receipt, committed };
}
describe('elastic native human proxy', () => {
  it('records a passive receipt and replays the exact tuple through the actual origin writer once', async () => {
    const origin = fixture(), coordinator = fixture();
    const asked = await origin.repo.create({ sourceSession: 'manager@worker', destinationSession: 'operator@external', body: 'Proceed?',
      summary: 'Decision', evidenceRef: 'fixture:evidence', humanIntent: 'decision', humanQuestions: [question], nudge: false });
    const proxy = coordinator.repo.createElasticHumanProxy({ worker, origin: origin.repo.getById(asked.qitemId)!, coordinatorManager: 'manager@coordinator', coordinatorHuman: 'human@kernel' });
    const input = { qitemId: proxy.qitemId, requestRevision: proxy.humanRequestRevision!, actorSession: 'human@kernel', replyId: 'answer-1',
      response: { kind: 'choice' as const, questionId: 'go', optionId: 'yes' } };
    const receipt = coordinator.repo.recordHumanResponse(input);
    expect(receipt.forwardingStatus).toBe('pending');
    expect(coordinator.repo.getById(receipt.replyQitemId)).toMatchObject({ state: 'blocked', destinationSession: 'manager@coordinator', tier: null, humanIntent: null, humanQuestions: null });
    const [intent] = coordinator.repo.pendingElasticHumanForwards();
    const applied = origin.repo.recordHumanResponse(intent.originInput);
    expect(origin.repo.recordHumanResponse(intent.originInput)).toEqual(applied);
    coordinator.repo.settleElasticHumanForward({ intentId: intent.intentId, worker, commit: origin.repo.humanResponseCommitEvidence(intent.originInput), origin: origin.repo.getById(asked.qitemId)! });
    expect(coordinator.repo.recordHumanResponse(input).forwardingStatus).toBe('origin-applied');
    expect(coordinator.repo.getById(receipt.replyQitemId)?.state).toBe('done');
    expect(coordinator.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 0 });
    expect(origin.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 1 });
  });
  it('serializes partial answers, suppresses relay retries, then supersedes from exact readback', async () => {
    const { origin, coordinator, proxy } = await pair([question, { ...question, id: 'when', question: 'When?' }]);
    const relay = new HumanRelayService(coordinator.db, coordinator.repo, { hostId: () => 'coordinator', bindings: () => [{ connectorId: 'hermes', profile: 'default', humanSession: 'human@kernel', chatId: 'chat', senderId: 'person' }] });
    expect(relay.pull({ connectorId: 'hermes', profile: 'default' }).deliveries).toHaveLength(1);
    const receipt = coordinator.repo.recordHumanResponse(answer(proxy));
    expect(receipt.complete).toBe(false);
    expect(coordinator.repo.getById(proxy.qitemId)?.humanForwardingStatus).toBe('pending');
    expect(relay.pull({ connectorId: 'hermes', profile: 'default' }).deliveries).toEqual([]);
    const current = coordinator.repo.getById(proxy.qitemId)!;
    expect(() => coordinator.repo.recordHumanResponse({ ...answer(current, 'two'), response: { kind: 'choice', questionId: 'when', optionId: 'no' } })).toThrow(/reconciliation/);
    const [intent] = coordinator.repo.pendingElasticHumanForwards();
    const { committed } = settle(origin, coordinator, intent);
    coordinator.repo.settleElasticHumanForward(committed);
    expect(coordinator.repo.pendingElasticHumanForwards()).toEqual([]);
    const input = { proxyQitemId: proxy.qitemId, worker, origin: committed.origin, disposition: 'remaining' as const };
    const next = coordinator.repo.reconcileElasticHumanProxy(input)!;
    expect(coordinator.repo.reconcileElasticHumanProxy(input)?.qitemId).toBe(next.qitemId);
    expect(next.qitemId).not.toBe(proxy.qitemId);
    expect(next.humanQuestions?.map(q => q.id)).toEqual(['when']);
    expect(coordinator.repo.getById(proxy.qitemId)?.state).toBe('canceled');
    coordinator.repo.recordHumanResponse({ ...answer(next, 'two'), response: { kind: 'custom', text: 'Tomorrow' } });
    const [second] = coordinator.repo.pendingElasticHumanForwards();
    settle(origin, coordinator, second);
    expect(origin.repo.getById('qitem-original')?.humanAnswers?.when).toMatchObject({ text: 'Tomorrow' });
    expect(coordinator.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 0 });
    expect(origin.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 2 });
    expect(relay.pull({ connectorId: 'hermes', profile: 'default' }).deliveries).toEqual([]);
  });

  it('retains clarification until the actual manager changes the question and advances a durable sequence', async () => {
    const { origin, coordinator, proxy } = await pair();
    coordinator.repo.recordHumanResponse({ ...answer(proxy), response: { kind: 'clarification', text: 'Why?' } });
    const [intent] = coordinator.repo.pendingElasticHumanForwards();
    const { committed } = settle(origin, coordinator, intent);
    expect(committed.origin.humanRequestRevision).toBe(intent.originInput.requestRevision);
    expect(() => coordinator.repo.reconcileElasticHumanProxy({ proxyQitemId: proxy.qitemId, worker, origin: committed.origin, disposition: 'remaining' })).toThrow(/changed pending/);
    origin.repo.update({ qitemId: 'qitem-original', actorSession: 'manager@worker', summary: 'Proceed only for synthetic test users' });
    const next = coordinator.repo.reconcileElasticHumanProxy({ proxyQitemId: proxy.qitemId, worker, origin: origin.repo.getById('qitem-original')!, disposition: 'remaining' })!;
    expect(next.qitemId).not.toBe(proxy.qitemId);
    expect(coordinator.db.prepare('SELECT sequence FROM elastic_human_proxies ORDER BY sequence').all()).toEqual([{ sequence: 0 }, { sequence: 1 }]);
    coordinator.repo.recordHumanResponse(answer(next, 'second'));
    const [second] = coordinator.repo.pendingElasticHumanForwards();
    expect(second.originInput.replyId).not.toBe(intent.originInput.replyId);
    settle(origin, coordinator, second);
  });

  it('refuses all generic mutation, activation, handoff, legacy answer and dependency paths', async () => {
    const { coordinator, proxy } = await pair();
    const receipt = coordinator.repo.recordHumanResponse(answer(proxy));
    for (const qitemId of [proxy.qitemId, receipt.replyQitemId]) {
      expect(() => coordinator.repo.stageWakeIntent(qitemId, 'human@kernel', 'manager@coordinator', null, true)).toThrow(/typed response/);
      await expect(coordinator.repo.maybeNudge(qitemId, 'manager@coordinator', true)).rejects.toThrow(/typed response/);
      expect(() => coordinator.repo.claim({ qitemId, destinationSession: coordinator.repo.getById(qitemId)!.destinationSession })).toThrow(/typed response/);
      expect(() => coordinator.repo.update({ qitemId, actorSession: 'manager@coordinator', state: 'pending' })).toThrow(/typed response/);
      expect(() => coordinator.repo.update({ qitemId, actorSession: 'manager@coordinator', state: 'done', closureReason: 'no-follow-on' })).toThrow(/typed response/);
      await expect(coordinator.repo.handoff({ qitemId, fromSession: 'manager@coordinator', toSession: 'other@coordinator' })).rejects.toThrow(/typed response/);
      await expect(coordinator.repo.handoffAndComplete({ qitemId, fromSession: 'manager@coordinator', toSession: 'other@coordinator' })).rejects.toThrow(/typed response/);
      expect(() => coordinator.repo.activateHumanResponseReturn({ qitemId, actorSession: 'human@kernel', fingerprint: 'x' })).toThrow(/typed response/);
      expect(() => coordinator.repo.resolveDirectHumanReply({ qitemId, actorSession: 'human@kernel', decision: 'yes' })).toThrow(/typed response/);
      expect(() => coordinator.repo.recordHumanAnswer({ qitemId, actorSession: 'human@kernel', questionId: 'go', optionId: 'yes' })).toThrow(/typed response/);
      expect(() => coordinator.db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id=?").run(qitemId)).toThrow(/elastic_human_transport_guard/);
      expect(() => coordinator.db.prepare("INSERT OR REPLACE INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,'','','human@kernel','manager@coordinator','pending','replaced')").run(qitemId)).toThrow(/elastic_human_transport_guard/);
    }
    const ordinary = await coordinator.repo.create({ sourceSession: 'other@coordinator', destinationSession: 'other@coordinator', body: 'Ordinary work', nudge: false });
    for (const blockedOn of [proxy.qitemId, receipt.replyQitemId]) {
      expect(() => coordinator.repo.update({ qitemId: ordinary.qitemId, actorSession: 'other@coordinator', state: 'blocked', blockedOn })).toThrow(/typed response/);
      expect(() => coordinator.db.prepare("UPDATE queue_items SET state='blocked',blocked_on=? WHERE qitem_id=?").run(blockedOn, ordinary.qitemId)).toThrow(/elastic_human_transport_dependency/);
    }
    await expect(coordinator.repo.create({ sourceSession: 'human@kernel', destinationSession: 'manager@coordinator', body: 'legacy thread answer', inboundReplyTo: proxy.qitemId })).rejects.toThrow(/typed response/);
    expect(coordinator.repo.listAttention().some(q => q.qitemId === receipt.replyQitemId)).toBe(false);
    expect(coordinator.repo.getById(receipt.replyQitemId)?.humanRequestRevision).toBeUndefined();
  });

  it('rolls back mapping/question atomically and rolls back response/answers on outbox conflict', async () => {
    const { origin, coordinator, proxy } = await pair();
    const q = origin.repo.getById('qitem-original')!;
    const fresh = fixture();
    expect(() => fresh.repo.createElasticHumanProxy({ worker, origin: { ...q, summary: null }, coordinatorManager: 'manager@coordinator', coordinatorHuman: 'human@kernel' })).toThrow(/summary/);
    expect(fresh.db.prepare('SELECT count(*) n FROM elastic_human_proxies').get()).toEqual({ n: 0 });
    expect(fresh.db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual({ n: 0 });
    const input = answer(proxy);
    const receiptId = `human-reply-${createHash('sha256').update(JSON.stringify([proxy.qitemId, input.replyId])).digest('hex')}`;
    new OutboxHandler(coordinator.db).record({ outboxId: `elastic-human-forward-${receiptId}`, senderSession: 'human@kernel', destinationSession: 'manager@coordinator', body: 'conflicting immutable payload' });
    expect(() => coordinator.repo.recordHumanResponse(input)).toThrow(/immutable tuple/);
    expect(coordinator.repo.getById(proxy.qitemId)).toMatchObject({ state: 'pending', humanAnswers: null });
    expect(coordinator.repo.getById(receiptId)).toBeNull();
    expect(coordinator.db.prepare('SELECT state,intent_id FROM elastic_human_proxies').get()).toEqual({ state: 'open', intent_id: null });
    const noOutbox = new QueueRepository(coordinator.db, new EventBus(coordinator.db), { validateRig: () => true, elasticHumanProxyAuthority: authority });
    expect(() => noOutbox.recordHumanResponse({ ...input, replyId: 'rollback' })).toThrow(/outbox/);
    expect(coordinator.repo.getById(proxy.qitemId)?.humanAnswers).toBeNull();
  });

  it('retains exact intent across disk restart, unavailable authority and old incarnation', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'elastic-human-proxy-'));
    try {
      const { origin, coordinator, proxy } = await pair();
      coordinator.repo.recordHumanResponse(answer(proxy));
      const [before] = coordinator.repo.pendingElasticHumanForwards();
      const file = join(dir, 'native.sqlite'); await coordinator.db.backup(file);
      const raw = createDb(file); databases.push(raw);
      expect(() => raw.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id=?").run(before.localReceiptQitemId)).toThrow(/elastic_human_transport_guard/);
      expect(raw.prepare('SELECT enabled FROM elastic_human_write_scope').get()).toEqual({ enabled: 0 });
      const restarted = fixture(file);
      expect(restarted.repo.pendingElasticHumanForwards()).toEqual([before]);
      const unavailable = repository(restarted.db, { ...authority, currentWorker: () => null });
      expect(() => unavailable.pendingElasticHumanForwards()).toThrow(/enrollment/);
      expect(() => unavailable.recordHumanResponse(answer(proxy))).toThrow(/enrollment/);
      const old = repository(restarted.db, { ...authority, currentWorker: () => ({ ...worker, vmId: 'replacement' }) });
      expect(() => old.pendingElasticHumanForwards()).toThrow(/enrollment/);
      const working = repository(restarted.db);
      // Simulate lost HTTP acknowledgment: origin already committed; replay exactly before settling.
      const receipt = origin.repo.recordHumanResponse(before.originInput);
      expect(origin.repo.recordHumanResponse(before.originInput)).toEqual(receipt);
      working.settleElasticHumanForward({ intentId: before.intentId, worker, commit: origin.repo.humanResponseCommitEvidence(before.originInput), origin: origin.repo.getById('qitem-original')! });
      expect(working.pendingElasticHumanForwards()).toEqual([]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it('requires positive enrollment, real manager and explicit human identity and keeps forged tags ordinary', async () => {
    const { origin } = await pair();
    const q = origin.repo.getById('qitem-original')!;
    for (const trusted of [undefined, { ...authority, isLocalManager: () => false }, { ...authority, isOriginManager: () => false }, { ...authority, isHumanBinding: () => false }]) {
      const f = fixture();
      const repo = new QueueRepository(f.db, new EventBus(f.db), { validateRig: () => true, elasticHumanProxyAuthority: trusted });
      expect(() => repo.createElasticHumanProxy({ worker, origin: q, coordinatorManager: 'manager@coordinator', coordinatorHuman: 'human@kernel' })).toThrow(/enrollment/);
    }
    const ordinary = fixture();
    const fake = await ordinary.repo.create({ sourceSession: 'manager@coordinator', destinationSession: 'human@kernel', body: 'elastic proxy claimed in body', tags: ['elastic-human-forward', 'elastic-proxy'], summary: 'Question', evidenceRef: 'fixture', humanQuestions: [question], nudge: false });
    const receipt = ordinary.repo.recordHumanResponse(answer(fake));
    expect(receipt.forwardingStatus).toBeUndefined();
    expect(ordinary.repo.getById(receipt.replyQitemId)?.state).toBe('pending');
    expect(ordinary.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 1 });
  });

  it('does not overwrite direct origin answer/revision/cancellation winners or accept a mismatched receipt', async () => {
    for (const winner of ['answer', 'revision', 'cancel']) {
      const { origin, coordinator, proxy } = await pair();
      const input = answer(proxy); coordinator.repo.recordHumanResponse(input);
      const [intent] = coordinator.repo.pendingElasticHumanForwards();
      if (winner === 'answer') origin.repo.recordHumanResponse({ ...intent.originInput, replyId: 'direct-origin', response: { kind: 'choice', questionId: 'go', optionId: 'no' } });
      else origin.repo.update({ qitemId: 'qitem-original', actorSession: 'manager@worker', ...(winner === 'cancel' ? { state: 'canceled' as const } : { summary: 'Changed native question' }) });
      expect(() => origin.repo.recordHumanResponse(intent.originInput)).toThrow(/pending|changed/);
      coordinator.repo.reconcileElasticHumanProxy({ proxyQitemId: proxy.qitemId, worker, origin: origin.repo.getById('qitem-original')!, disposition: 'origin-changed' });
      expect(coordinator.repo.getById(proxy.qitemId)?.humanForwardingStatus).toBe('conflict');
      expect(coordinator.repo.getById(input.qitemId)?.humanAnswers?.go).toBe('yes');
      expect(coordinator.db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get(intent.intentId)).toEqual({ delivery_state: 'pending' });
    }
    const { origin, coordinator, proxy } = await pair(); coordinator.repo.recordHumanResponse(answer(proxy));
    const [intent] = coordinator.repo.pendingElasticHumanForwards(); origin.repo.recordHumanResponse(intent.originInput);
    const commit = origin.repo.humanResponseCommitEvidence(intent.originInput);
    expect(() => coordinator.repo.settleElasticHumanForward({ intentId: intent.intentId, worker, commit: { ...commit, fingerprint: 'wrong' }, origin: origin.repo.getById('qitem-original')! })).toThrow(/committed tuple/);
    expect(coordinator.repo.pendingElasticHumanForwards()).toHaveLength(1);
    expect(() => coordinator.repo.recordHumanResponse({ ...answer(proxy), response: { kind: 'custom', text: 'Changed replay' } })).toThrow(/replyId/);
  });

  it('exposes exact committed receipt evidence through authenticated native HTTP with original actor binding', async () => {
    const { origin, coordinator, proxy } = await pair(); coordinator.repo.recordHumanResponse(answer(proxy));
    const [intent] = coordinator.repo.pendingElasticHumanForwards();
    const app = new Hono().route('/api/mission-control', humanResponsesRoutes({ queueRepo: origin.repo, bearerToken: 'worker-scoped' }));
    const post = (path: string, input: unknown, headers: Record<string, string> = {}) => app.request(`/api/mission-control/${path}`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(input) });
    expect((await post('human-response-receipt', intent.originInput)).status).toBe(401);
    const headers = { Authorization: 'Bearer worker-scoped', 'X-OpenRig-Session': 'operator@external', 'X-OpenRig-Relay': 'coordinator', 'X-OpenRig-Provenance': 'claimed:v1' };
    expect((await post('human-response-receipt', intent.originInput, headers)).status).toBe(409);
    expect((await post('human-response', intent.originInput, headers)).status).toBe(200);
    const read = await post('human-response-receipt', intent.originInput, headers);
    expect(read.status).toBe(200);
    expect(await read.json()).toEqual(origin.repo.humanResponseCommitEvidence(intent.originInput));
    expect((await post('human-response-receipt', intent.originInput, { ...headers, 'X-OpenRig-Session': 'manager@coordinator' })).status).toBe(409);
    expect((await post('human-response-receipt', { ...intent.originInput, response: { kind: 'custom', text: 'Forgery' } }, headers)).status).toBe(409);
    expect(origin.repo.transitionLog.listForQitem('qitem-original').at(-1)?.identityProvenance).toBe('claimed:v1');
  });

  it('refuses preexisting dependencies atomically and leaves ordinary raw SQL writes usable', async () => {
    const { coordinator, proxy } = await pair();
    const input = answer(proxy);
    const receiptId = `human-reply-${createHash('sha256').update(JSON.stringify([proxy.qitemId, input.replyId])).digest('hex')}`;
    const work = await coordinator.repo.create({ sourceSession: 'other@coordinator', destinationSession: 'other@coordinator', body: 'Work', nudge: false });
    coordinator.db.prepare("UPDATE queue_items SET state='blocked',blocked_on=? WHERE qitem_id=?").run(receiptId, work.qitemId);
    expect(() => coordinator.repo.recordHumanResponse(input)).toThrow(/executable coordinator work/);
    expect(coordinator.repo.getById(work.qitemId)).toMatchObject({ state: 'blocked', blockedOn: receiptId });
    expect(coordinator.repo.getById(proxy.qitemId)?.state).toBe('pending');
    expect(coordinator.db.prepare('SELECT count(*) n FROM outbox_entries').get()).toEqual({ n: 0 });
    expect(coordinator.db.prepare('SELECT enabled FROM elastic_human_write_scope').get()).toEqual({ enabled: 0 });
    coordinator.db.prepare("UPDATE queue_items SET state='pending',blocked_on=NULL WHERE qitem_id=?").run(work.qitemId);
    expect(coordinator.repo.getById(work.qitemId)?.state).toBe('pending');
  });

  it('retains clarification when a direct human answer changes only origin answers', async () => {
    const { origin, coordinator, proxy } = await pair([question, { ...question, id: 'when' }]);
    coordinator.repo.recordHumanResponse({ ...answer(proxy), response: { kind: 'clarification', text: 'Why?' } });
    const [intent] = coordinator.repo.pendingElasticHumanForwards(); settle(origin, coordinator, intent);
    origin.repo.recordHumanResponse({ ...intent.originInput, replyId: 'direct', response: { kind: 'choice', questionId: 'go', optionId: 'no' } });
    expect(() => coordinator.repo.reconcileElasticHumanProxy({ proxyQitemId: proxy.qitemId, worker, origin: origin.repo.getById('qitem-original')!, disposition: 'remaining' })).toThrow(/actual manager question change/);
    coordinator.repo.reconcileElasticHumanProxy({ proxyQitemId: proxy.qitemId, worker, origin: origin.repo.getById('qitem-original')!, disposition: 'origin-changed' });
    expect(coordinator.repo.getById(proxy.qitemId)?.humanForwardingStatus).toBe('conflict');
  });

});
