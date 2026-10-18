import type Database from 'better-sqlite3';
import { z } from 'zod';
import { ElasticWorkerControl, workerDigest } from '../domain/elastic-worker-control.js';
import { nativeContinuation, issueOwnedSuccessor, readContinuationAction } from '../domain/elastic-continuation.js';
import { continuationManifest, observeContinuationTarget, reconcileOwnedContinuation, validateWithdrawalTarget } from './elastic-owned-continuation.js';
import type { TmuxAdapter } from './tmux.js';

/** One worker-native queue and target lease. No supplied text, callback, or fresh conversation. */
export class ElasticContinuationExecutor {
  constructor(private readonly db:Database.Database,private readonly tmux:TmuxAdapter) {}
  inspect() {
    const control=new ElasticWorkerControl(this.db),state=control.read(),generation=control.generation();
    if(!state||state.phase!=='draining')throw Error('elastic_continuation_unavailable');
    const candidates=[];
    const rows=z.array(z.object({outbox_id:z.string()})).parse(this.db.prepare(`SELECT n.outbox_id FROM elastic_native_wakes n JOIN outbox_entries o USING(outbox_id)
      WHERE n.generation=? AND o.delivery_state IN ('pending','retained') AND NOT EXISTS(SELECT 1 FROM elastic_continuation_actions a WHERE a.outbox_id=n.outbox_id)
      ORDER BY n.outbox_id LIMIT 20`).all(state.generation));
    for(const row of rows) {
      try {const {native}=nativeContinuation(this.db,row.outbox_id),target=observeContinuationTarget(this.db,row.outbox_id,continuationManifest());
        candidates.push({outboxId:row.outbox_id,queueDigest:native.queue_digest,target});} catch { /* One stale lineage cannot select a different target. */ }
    }
    const actions=z.array(z.object({action_id:z.string(),stage:z.string()})).parse(this.db.prepare('SELECT action_id,stage FROM elastic_continuation_actions WHERE generation=? ORDER BY created_at').all(state.generation));
    return {state,generation,candidates,actions,dispositionDigest:control.ledger().digest};
  }
  async executeOwnedContinuation(actionId:string) {
    const before=readContinuationAction(this.db,actionId);
    if(before.stage==='prepared')await this.tmux.sendOwnedContinuation(actionId);
    return {actionId,result:reconcileOwnedContinuation(this.db,actionId)};
  }
  async advance(withdrawalId:string) {
    const view=this.inspect();
    if(view.generation?.purpose!=='continuation'||view.generation.withdrawal_id!==withdrawalId||view.generation.retired_at)throw Error('elastic_continuation_generation');
    const action=view.actions.find(a=>a.stage!=='proved');
    if(action)return this.executeOwnedContinuation(action.action_id);
    for(const candidate of view.candidates) {
      const manifest=continuationManifest();
      const kind=validateWithdrawalTarget(this.db,candidate.outboxId,candidate.target,manifest);
      const issued=issueOwnedSuccessor(this.db,candidate.outboxId,candidate.target,kind);
      return this.executeOwnedContinuation(issued.actionId);
    }
    const control=new ElasticWorkerControl(this.db);
    return {dispositionDigest:workerDigest({generation:view.generation,ledger:control.ledger()}),idle:control.ledger().clean};
  }
}
