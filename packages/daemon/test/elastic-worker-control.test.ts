import { afterEach, describe, expect, it } from 'vitest';
import Database from 'better-sqlite3';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { migrate } from '../src/db/migrate.js';
import { ElasticWorkerControl } from '../src/domain/elastic-worker-control.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { EventBus } from '../src/domain/event-bus.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
const identity = { workerId: 'worker-a', vmId: 'a0000000-0000-0000-0000-000000000001', manifestDigest: 'a'.repeat(64) };
const cleanup: Database.Database[] = [];
afterEach(() => { for (const db of cleanup.splice(0)) db.close(); });
function fixture() { const db = new Database(':memory:'); cleanup.push(db); migrate(db, ALL_MIGRATIONS); const control = new ElasticWorkerControl(db); const repo = new QueueRepository(db, new EventBus(db)); repo.attachOutbox(new OutboxHandler(db)); return { db, control, repo }; }
const input = { sourceSession: 'owner@rig', destinationSession: 'owner@rig', body: 'work' };
describe('elastic native worker fence', () => {
  it('leaves unenrolled workers unchanged and rejects incarnation replacement', async () => {
    const { control, repo } = fixture(); await repo.create(input); expect(control.read()).toBeNull();
    control.enroll(identity); expect(control.enroll(identity)).toMatchObject({ phase: 'ready', generation: 0 });
    expect(() => control.enroll({ ...identity, vmId: 'b0000000-0000-0000-0000-000000000001' })).toThrow('elastic_worker_identity');
  });
  it('fences creation, direct insert and forged lineage while baseline claims remain permitted', async () => {
    const { db, control, repo } = fixture(); control.enroll(identity); const item = await repo.create(input);
    const drain = control.beginDrain(identity, 'drain-a'); expect(drain.generation).toBe(1);
    expect(control.beginDrain(identity, 'drain-a')).toEqual(drain);
    await expect(repo.create({ ...input, handedOffFrom: item.qitemId, chainOfRecord: [item.qitemId] })).rejects.toThrow('elastic_worker_fenced');
    expect(() => db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES ('fake','','','owner@rig','owner@rig','pending','x')").run()).toThrow('elastic_worker_fenced');
    expect(repo.claim({ qitemId: item.qitemId, destinationSession: 'owner@rig' }).state).toBe('in-progress');
  });
  it('admits native handoffs only from live baseline ownership and prevents completed baseline reopen', async () => {
    const { db, control, repo } = fixture(); control.enroll(identity); const item = await repo.create(input); control.beginDrain(identity, 'drain-a');
    await expect(repo.handoff({ qitemId: item.qitemId, fromSession: 'intruder@rig', toSession: 'next@rig', nudge: false })).rejects.toThrow('elastic_worker_lineage');
    const result = await repo.handoff({ qitemId: item.qitemId, fromSession: 'owner@rig', toSession: 'next@rig', nudge: false });
    expect(repo.claim({ qitemId: result.created.qitemId, destinationSession: 'next@rig' }).state).toBe('in-progress');
    expect(() => db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id=?").run(item.qitemId)).toThrow('elastic_worker_fenced');
  });
  it('seals durably and denies claims even after constructing a new repository', async () => {
    const { db, control, repo } = fixture(); control.enroll(identity); const item = await repo.create(input); control.beginDrain(identity, 'drain-a');
    expect(() => control.sealAdmission(identity, 2)).toThrow('elastic_worker_generation');
    control.sealAdmission(identity, 1); expect(new ElasticWorkerControl(db).read()?.phase).toBe('sealed');
    expect(() => new QueueRepository(db, new EventBus(db)).claim({ qitemId: item.qitemId, destinationSession: 'owner@rig' })).toThrow('elastic_worker_fenced');
  });
  it('counts all custody independently of runnable slots and due times', async () => {
    const { db, control, repo } = fixture(); control.enroll(identity);
    for (let i = 0; i < 205; i++) { const q = await repo.create({ ...input, destinationSession: 'archived@old-rig' }); db.prepare("UPDATE queue_items SET state='blocked',blocked_on='external:human-response-commit:later' WHERE qitem_id=?").run(q.qitemId); }
    db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES ('human-response-activation-later','a','b','{}','2999-01-01','pending')").run();
    const probe = control.ledger(); expect(probe.complete).toBe(true); expect(probe.custody).toBe(205); expect(probe.runnable).toBe(0); expect(probe.pendingHumanReturns).toBe(true); expect(probe.pendingActivationIntents).toBe(true); expect(probe.clean).toBe(false);
  });
  it('returns exact same-ID create retries during drain without admitting changed identities', async () => {
    const { control, repo } = fixture(); control.enroll(identity); const item = await repo.create({ ...input, qitemId: 'repeat' });
    control.beginDrain(identity, 'drain-a'); expect((await repo.create({ ...input, qitemId: 'repeat' })).qitemId).toBe(item.qitemId);
    await expect(repo.create({ ...input, qitemId: 'repeat', destinationSession: 'other@rig' })).rejects.toThrow();
  });
  it('preserves native local human-answer continuation and idempotent receipts during drain', async () => {
    const { control, repo } = fixture(); control.enroll(identity);
    const question = await repo.create({ sourceSession: 'owner@rig', destinationSession: 'human@kernel', body: 'Decide', summary: 'Decision', evidenceRef: '/proof', humanIntent: 'decision', nudge: false });
    control.beginDrain(identity, 'drain-a');
    const answer = { qitemId: question.qitemId, requestRevision: question.humanRequestRevision!, actorSession: 'human@kernel', replyId: 'one', response: { kind: 'custom' as const, text: 'Yes' } };
    const receipt = repo.recordHumanResponse(answer); expect(receipt.complete).toBe(true); expect(repo.recordHumanResponse(answer)).toEqual(receipt);
    expect(repo.claim({ qitemId: receipt.replyQitemId, destinationSession: 'owner@rig' }).state).toBe('in-progress');
  });
  it('activates an existing provisional return but rejects new remote lineage during drain', () => {
    const { control, repo } = fixture(); control.enroll(identity);
    const staged = repo.stageHumanResponseReturn({ ...input, qitemId: 'staged', tags: ['response:fingerprint'] });
    control.beginDrain(identity, 'drain-a');
    expect(repo.activateHumanResponseReturn({ qitemId: staged.qitemId, actorSession: input.sourceSession, fingerprint: 'fingerprint' }).state).toBe('pending');
    expect(() => repo.stageHumanResponseReturn({ ...input, qitemId: 'new-foreign', tags: ['response:fingerprint'], chainOfRecord: [staged.qitemId] })).toThrow('elastic_worker_fenced');
  });

  it('fences independent SQLite connections and preserves direct-SQL custody against replace/delete', async () => {
    const { db, control, repo } = fixture(); control.enroll(identity); const item = await repo.create(input);
    const dir = mkdtempSync(join(tmpdir(), 'elastic-connections-')); const file = join(dir, 'db.sqlite');
    await db.backup(file); const first = new Database(file); const second = new Database(file);
    try {
      const otherRepo = new QueueRepository(second, new EventBus(second));
      new ElasticWorkerControl(first).beginDrain(identity, 'drain-a');
      await expect(otherRepo.create(input)).rejects.toThrow('elastic_worker_fenced');
      expect(() => second.prepare('DELETE FROM queue_items WHERE qitem_id=?').run(item.qitemId)).toThrow('elastic_worker_fenced');
      expect(() => second.prepare("INSERT OR REPLACE INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,'','','owner@rig','owner@rig','pending','replacement')").run(item.qitemId)).toThrow('elastic_worker_fenced');
      new ElasticWorkerControl(first).sealAdmission(identity, 1);
      expect(() => otherRepo.claim({ qitemId: item.qitemId, destinationSession: 'owner@rig' })).toThrow('elastic_worker_fenced');
    } finally { second.close(); first.close(); rmSync(dir, { recursive: true, force: true }); }
  });
  it('extends handoff-and-complete custody atomically and rolls back forbidden successor work', async () => {
    const { control, repo } = fixture(); control.enroll(identity); const item = await repo.create(input); control.beginDrain(identity, 'drain-a');
    const handoff = await repo.handoffAndComplete({ qitemId: item.qitemId, fromSession: 'owner@rig', toSession: 'next@rig', nudge: false });
    expect(handoff.closed.state).toBe('done'); expect(repo.claim({qitemId:handoff.created.qitemId,destinationSession:'next@rig'}).state).toBe('in-progress');
  });

});
