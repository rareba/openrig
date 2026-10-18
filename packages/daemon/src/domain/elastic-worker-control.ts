import { isHumanSeatSessionRef } from './session-name.js';
import type Database from 'better-sqlite3';

export interface ElasticWorkerIdentity { workerId: string; vmId: string; manifestDigest: string }
export interface ElasticWorkerState extends ElasticWorkerIdentity {
  version: 1; phase: 'ready' | 'draining' | 'sealed'; generation: number;
  drainOperationId: string | null; drainStartedAt: number | null;
}
export interface ElasticWorkerLedger {
  complete: boolean; custody: number; runnable: number; claims: number; unresolvedOutbox: number;
  pendingHumanReturns: boolean; pendingActivationIntents: boolean; pendingTransfers: boolean;
  clean: boolean; digest: string;
}
import { createHash } from 'node:crypto';
export function workerDigest(value: unknown): string { return createHash('sha256').update(JSON.stringify(value)).digest('hex'); }
function identityValid(value: ElasticWorkerIdentity): boolean {
  return typeof value.workerId === 'string' && value.workerId.length > 0 && value.workerId.length <= 1024
    && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(value.vmId) && /^[a-f0-9]{64}$/.test(value.manifestDigest);
}
export class ElasticWorkerControl {
  constructor(readonly db: Database.Database, private readonly now: () => number = Date.now) {}
  read(): ElasticWorkerState | null {
    const row = this.db.prepare(`SELECT worker_id AS workerId, vm_id AS vmId, manifest_digest AS manifestDigest,
      phase, generation, drain_operation_id AS drainOperationId, drain_started_at AS drainStartedAt
      FROM elastic_worker_control WHERE id=1`).get() as Omit<ElasticWorkerState, 'version'> | undefined;
    return row ? { version: 1, ...row } : null;
  }
  private require(identity: ElasticWorkerIdentity): ElasticWorkerState {
    const state = this.read();
    if (!state || !identityValid(identity) || state.workerId !== identity.workerId || state.vmId !== identity.vmId || state.manifestDigest !== identity.manifestDigest) throw Error('elastic_worker_identity');
    return state;
  }
  enroll(identity: ElasticWorkerIdentity): ElasticWorkerState {
    return this.db.transaction(() => {
      if (!identityValid(identity)) throw Error('elastic_worker_identity');
      if (this.read()) return this.require(identity);
      this.db.prepare("INSERT INTO elastic_worker_control(id,worker_id,vm_id,manifest_digest,phase) VALUES(1,?,?,?,'ready')").run(identity.workerId, identity.vmId, identity.manifestDigest);
      return this.require(identity);
    }).immediate();
  }
  beginDrain(identity: ElasticWorkerIdentity, operationId: string): ElasticWorkerState {
    return this.db.transaction(() => {
      const state = this.require(identity);
      if (typeof operationId !== 'string' || !operationId || operationId.length > 512) throw Error('elastic_worker_operation');
      if (state.phase !== 'ready') { if (state.drainOperationId !== operationId) throw Error('elastic_worker_operation'); return state; }
      this.db.prepare("UPDATE elastic_worker_control SET phase='draining',generation=generation+1,drain_operation_id=?,drain_started_at=? WHERE id=1").run(operationId, this.now());
      this.db.prepare("INSERT INTO elastic_worker_custody(qitem_id,generation) SELECT qitem_id,? FROM queue_items WHERE state IN ('pending','in-progress','blocked')").run(state.generation + 1);
      return this.require(identity);
    }).immediate();
  }
  /** Admission denial is irreversible; this alone is never removal proof. */
  sealAdmission(identity: ElasticWorkerIdentity, generation: number): ElasticWorkerState {
    return this.db.transaction(() => {
      const state = this.require(identity);
      if (state.phase === 'ready' || state.generation !== generation) throw Error('elastic_worker_generation');
      this.db.prepare("UPDATE elastic_worker_control SET phase='sealed' WHERE id=1").run(); return this.require(identity);
    }).immediate();
  }
  ledger(): ElasticWorkerLedger {
    return this.db.transaction(() => {
      // No UI pagination, seat/archive join or due-time filter: all durable native obligations count.
      const queue = this.db.prepare('SELECT * FROM queue_items ORDER BY qitem_id').all() as Record<string, unknown>[];
      const outbox = this.db.prepare('SELECT * FROM outbox_entries ORDER BY outbox_id').all() as Record<string, unknown>[];
      const active = queue.filter(row => ['pending','in-progress','blocked'].includes(String(row.state)));
      const unresolved = outbox.filter(row => !['delivered','retired'].includes(String(row.delivery_state)));
      const complete = queue.every(row => ['pending','in-progress','blocked','done','failed','denied','canceled','handed-off'].includes(String(row.state)));
      const claims = active.filter(row => row.state === 'in-progress').length;
      const pendingHumanReturns = active.some(row => String(row.blocked_on).startsWith('external:human-response-commit:') || isHumanSeatSessionRef(String(row.destination_session)));
      const pendingActivationIntents = unresolved.some(row => String(row.outbox_id).startsWith('human-response-activation-'));
      return { complete, custody: active.length, runnable: active.filter(row => row.state !== 'blocked').length, claims,
        unresolvedOutbox: unresolved.length, pendingHumanReturns, pendingActivationIntents, pendingTransfers: unresolved.length > 0,
        clean: complete && active.length === 0 && unresolved.length === 0, digest: workerDigest({ queue, outbox }) };
    })();
  }
}

/** Called ONLY inside a verified native handoff transaction, before its parent becomes terminal. */
export function authorizeElasticSuccessor(db: Database.Database, parentId: string, successorId: string, owner: string, destination: string, lineage: 'handoff' | 'human-response' = 'handoff'): void {
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='elastic_worker_control'").get()) return;
  const control = new ElasticWorkerControl(db); const state = control.read(); if (!state || state.phase === 'ready') return;
  if (!db.inTransaction) throw Error('elastic_worker_transaction');
  const parent = db.prepare(`SELECT q.qitem_id FROM queue_items q JOIN elastic_worker_custody p ON p.qitem_id=q.qitem_id
    WHERE q.qitem_id=? AND q.destination_session=? AND q.state IN ('pending','in-progress','blocked') AND p.generation=?`).get(parentId, owner, state.generation);
  if (state.phase !== 'draining' || !parent) throw Error('elastic_worker_lineage');
  db.prepare('INSERT INTO elastic_worker_custody(qitem_id,parent_id,generation,source_session,destination_session,lineage) VALUES(?,?,?,?,?,?)')
    .run(successorId, parentId, state.generation, owner, destination, lineage);
}
