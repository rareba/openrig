import type Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { ElasticWorkerControl, workerDigest } from './elastic-worker-control.js';

const text = z.string().min(1).max(4096);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
export const continuationProcessSchema = z.object({bootId:text,runnerPid:z.number().int().positive(),runnerStartTime:text,childPid:z.number().int().positive(),childStartTime:text}).strict();
export const continuationTargetSchema = z.object({nodeId:text,session:text,occupant:text,pane:text,conversation:text,sessionFile:text,
  stateRoot:text,cwd:text,model:z.string().nullable(),launchId:text,process:continuationProcessSchema}).strict();
export const withdrawalRequestSchema = z.object({identity:z.object({workerId:text,vmId:z.string().uuid(),manifestDigest:digest}).strict(),
  generation:z.number().int().positive(),operationId:text,withdrawalId:z.string().uuid(),outboxId:text,queueDigest:digest,target:continuationTargetSchema}).strict();
export type WithdrawalRequest = z.infer<typeof withdrawalRequestSchema>;
export type ContinuationTarget = z.infer<typeof continuationTargetSchema>;
export const continuationActionSchema = z.object({version:z.literal(1),actionId:z.string().uuid(),generation:z.number().int().positive(),withdrawalId:text,
  qitemId:text,parentId:text.nullable(),parentDigest:digest.nullable(),outboxId:text,queueDigest:digest,outboxDigest:digest,payload:text,payloadDigest:digest,target:continuationTargetSchema,
  effect:z.discriminatedUnion('kind',[
    z.object({kind:z.literal('existing-runner')}).strict(),
    z.object({kind:z.literal('resume-conversation'),launchId:z.string().uuid()}).strict(),
  ])}).strict();
export type ContinuationAction = z.infer<typeof continuationActionSchema>;
const stage = z.enum(['prepared','transport-issued','spawn-issued','input-issued','proved']);
export const continuationRowSchema = z.object({action_json:z.string(),stage,runner_json:z.string().nullable(),receipt_json:z.string().nullable()});
export type ContinuationStage = z.infer<typeof stage>;
function deny(): never {throw Error('elastic_continuation_denied');}

export function continuationQueue(db:Database.Database,qitemId:string) {
  return z.object({qitem_id:text,source_session:text,destination_session:text,state:text,body:z.string(),blocked_on:z.string().nullable(),
    claimed_by_generation_uuid:z.string().nullable(),human_questions:z.string().nullable(),human_answers:z.string().nullable(),human_intent:z.string().nullable(),tags:z.string().nullable(),chain_of_record:z.string().nullable(),summary:z.string().nullable(),handed_off_from:z.string().nullable(),reply_to:z.string().nullable(),
    ts_updated:text}).parse(db.prepare(`SELECT qitem_id,source_session,destination_session,state,body,blocked_on,claimed_by_generation_uuid,
      human_questions,human_answers,human_intent,tags,chain_of_record,summary,handed_off_from,reply_to,ts_updated FROM queue_items WHERE qitem_id=?`).get(qitemId));
}
function outbox(db:Database.Database,outboxId:string) {
  return z.object({outbox_id:text,audit_pointer:text,sender_session:text,destination_session:text,body:text,ts_dispatched:text})
    .parse(db.prepare('SELECT outbox_id,audit_pointer,sender_session,destination_session,body,ts_dispatched FROM outbox_entries WHERE outbox_id=?').get(outboxId));
}
/** Only native queue transactions call this; public outbox recording cannot mint this witness. */
export function recordElasticNativeWake(db:Database.Database,outboxId:string):void {
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE name='elastic_native_wakes'").get()) return;
  const control=new ElasticWorkerControl(db), state=control.read();
  if (!state || state.phase==='ready') return;
  if (!db.inTransaction || state.phase!=='draining') deny();
  const wake=outbox(db,outboxId), queue=continuationQueue(db,wake.audit_pointer);
  const custody=z.object({parent_id:z.string().nullable()}).parse(db.prepare('SELECT parent_id FROM elastic_worker_custody WHERE generation=? AND qitem_id=?').get(state.generation,queue.qitem_id));
  if (queue.destination_session!==wake.destination_session) deny();
  db.prepare('INSERT OR IGNORE INTO elastic_native_wakes(outbox_id,qitem_id,generation,parent_id,parent_digest,queue_digest,outbox_digest) VALUES(?,?,?,?,?,?,?)')
    .run(outboxId,queue.qitem_id,state.generation,custody.parent_id,custody.parent_id?workerDigest(continuationQueue(db,custody.parent_id)):null,workerDigest(queue),workerDigest(wake));
}
export function readContinuationAction(db:Database.Database,actionId:string) {
  const row=continuationRowSchema.parse(db.prepare('SELECT action_json,stage,runner_json,receipt_json FROM elastic_continuation_actions WHERE action_id=?').get(actionId));
  const action=continuationActionSchema.parse(JSON.parse(row.action_json));
  if(action.actionId!==actionId) deny();
  return {action,stage:row.stage,runner:row.runner_json ? continuationProcessSchema.parse(JSON.parse(row.runner_json)):null,receipt:row.receipt_json};
}
export function nativeContinuation(db:Database.Database,outboxId:string) {
  const native=z.object({qitem_id:text,generation:z.number().int(),parent_id:text.nullable(),parent_digest:digest.nullable(),queue_digest:digest,outbox_digest:digest})
    .parse(db.prepare('SELECT * FROM elastic_native_wakes WHERE outbox_id=?').get(outboxId));
  const queue=continuationQueue(db,native.qitem_id), wake=outbox(db,outboxId);
  if(native.parent_id && workerDigest(continuationQueue(db,native.parent_id))!==native.parent_digest)deny();
  if(workerDigest(queue)!==native.queue_digest || workerDigest(wake)!==native.outbox_digest
    || !['pending','in-progress'].includes(queue.state) || queue.destination_session!==wake.destination_session) deny();
  const delivery=z.object({delivery_state:text}).parse(db.prepare('SELECT delivery_state FROM outbox_entries WHERE outbox_id=?').get(outboxId));
  if (!['pending','retained'].includes(delivery.delivery_state)) deny();
  return {native,queue,wake};
}
export function assertContinuationTarget(db:Database.Database,target:ContinuationTarget):void {
  const rows=db.prepare(`SELECT n.id AS nodeId,s.session_name AS session,b.tmux_pane AS pane,s.resume_token AS sessionFile,n.cwd,n.model,
      (SELECT generation_uuid FROM occupant_tenures WHERE node_id=n.id ORDER BY generation_ordinal DESC LIMIT 1) AS occupant
    FROM nodes n JOIN sessions s ON s.node_id=n.id JOIN bindings b ON b.node_id=n.id
    WHERE n.id=? AND n.runtime='pi' AND s.resume_type='pi_session_file' ORDER BY s.id DESC LIMIT 1`).all(target.nodeId);
  const expected={nodeId:target.nodeId,session:target.session,pane:target.pane,sessionFile:target.sessionFile,cwd:target.cwd,model:target.model,occupant:target.occupant};
  if(rows.length!==1 || workerDigest(rows[0])!==workerDigest(expected)) deny();
}
/** Root calls inside its publication transaction. No caller payload becomes prompt text. */
export function issueWithdrawalAction(db:Database.Database,request:WithdrawalRequest,kind:'existing-runner'|'resume-conversation'):ContinuationAction {
  if(!db.inTransaction) deny();
  const control=new ElasticWorkerControl(db);
  const previous=db.prepare('SELECT action_id FROM elastic_continuation_actions WHERE withdrawal_id=?').get(request.withdrawalId);
  if(previous) {
    const id=z.object({action_id:text}).parse(previous).action_id;
    const generation=control.generation();
    if(generation?.selector_digest!==workerDigest(request))deny();
    const action=assertContinuationAction(db,id).action;
    if(action.effect.kind!==kind)deny();
    return action;
  }
  const {native,queue,wake}=nativeContinuation(db,request.outboxId);
  if(native.generation!==request.generation || native.queue_digest!==request.queueDigest || queue.destination_session!==request.target.session) deny();
  assertContinuationTarget(db,request.target);
  const state=control.withdraw(request.identity,{...request,selectorDigest:workerDigest(request),qitemId:queue.qitem_id});
  const action=continuationActionSchema.parse({version:1,actionId:randomUUID(),generation:state.generation,withdrawalId:request.withdrawalId,
    qitemId:queue.qitem_id,parentId:native.parent_id,parentDigest:native.parent_digest,outboxId:request.outboxId,queueDigest:native.queue_digest,outboxDigest:native.outbox_digest,
    payload:wake.body,payloadDigest:workerDigest(wake.body),target:request.target,effect:kind==='existing-runner'?{kind}:{kind,launchId:randomUUID()}});
  db.prepare("INSERT INTO elastic_continuation_actions(action_id,generation,withdrawal_id,qitem_id,outbox_id,action_json,stage,created_at) VALUES(?,?,?,?,?,?,'prepared',?)")
    .run(action.actionId,action.generation,action.withdrawalId,action.qitemId,action.outboxId,JSON.stringify(action),Date.now());
  return action;
}
export function assertContinuationAction(db:Database.Database,actionId:string) {
  const row=readContinuationAction(db,actionId),a=row.action,control=new ElasticWorkerControl(db),state=control.read(),generation=control.generation();
  if(state?.phase!=='draining' || state.generation!==a.generation || generation?.purpose!=='continuation' || generation.withdrawal_id!==a.withdrawalId || generation.retired_at) deny();
  const custody=db.prepare('SELECT parent_id FROM elastic_worker_custody WHERE generation=? AND qitem_id=?').get(a.generation,a.qitemId);
  if (!custody || z.object({parent_id:text.nullable()}).parse(custody).parent_id!==a.parentId) deny();
  assertContinuationTarget(db,a.target);
  if(a.parentId && workerDigest(continuationQueue(db,a.parentId))!==a.parentDigest)deny();
  const wake=outbox(db,a.outboxId);
  if(a.payload!==wake.body || workerDigest(wake)!==a.outboxDigest)deny();
  if(row.stage!=='proved' && workerDigest(continuationQueue(db,a.qitemId))!==a.queueDigest) deny();
  if(workerDigest(a.payload)!==a.payloadDigest) deny();
  return row;
}
export function continuationTransition(db:Database.Database,actionId:string,from:ContinuationStage,to:ContinuationStage):void {
  if(!db.inTransaction || db.prepare('UPDATE elastic_continuation_actions SET stage=? WHERE action_id=? AND stage=?').run(to,actionId,from).changes!==1) deny();
}

/** A successor inherits only a current native custody chain with observed ancestor progress. */
export function issueOwnedSuccessor(db:Database.Database,outboxId:string,target:ContinuationTarget,kind:'existing-runner'|'resume-conversation'):ContinuationAction {
  return db.transaction(()=>{
    const existing=db.prepare('SELECT action_id FROM elastic_continuation_actions WHERE outbox_id=?').get(outboxId);
    if(existing) {
      const action=assertContinuationAction(db,z.object({action_id:text}).parse(existing).action_id).action;
      if(workerDigest(action.target)!==workerDigest(target)||action.effect.kind!==kind)deny();
      return action;
    }
    const control=new ElasticWorkerControl(db),g=control.generation(),state=control.read();
    if(state?.phase!=='draining'||g?.purpose!=='continuation'||g.retired_at||!g.withdrawal_id)deny();
    const {native,queue,wake}=nativeContinuation(db,outboxId);
    if(native.generation!==g.generation||!native.parent_id||queue.destination_session!==target.session)deny();
    const lineage=db.prepare(`WITH RECURSIVE chain(qitem_id,parent_id) AS (
      SELECT qitem_id,parent_id FROM elastic_worker_custody WHERE generation=? AND qitem_id=?
      UNION SELECT p.qitem_id,p.parent_id FROM elastic_worker_custody p JOIN chain c ON p.qitem_id=c.parent_id WHERE p.generation=?
    ) SELECT 1 FROM chain c JOIN elastic_continuation_actions a ON a.qitem_id=c.qitem_id AND a.generation=? AND a.stage='proved' LIMIT 1`).get(g.generation,native.parent_id,g.generation,g.generation);
    if(!lineage || db.prepare("SELECT 1 FROM elastic_continuation_actions WHERE generation=? AND stage!='proved'").get(g.generation))deny();
    assertContinuationTarget(db,target);
    const action=continuationActionSchema.parse({version:1,actionId:randomUUID(),generation:g.generation,withdrawalId:g.withdrawal_id,
      qitemId:queue.qitem_id,parentId:native.parent_id,parentDigest:native.parent_digest,outboxId,queueDigest:native.queue_digest,outboxDigest:native.outbox_digest,
      payload:wake.body,payloadDigest:workerDigest(wake.body),target,effect:kind==='existing-runner'?{kind}:{kind,launchId:randomUUID()}});
    db.prepare("INSERT INTO elastic_continuation_actions(action_id,generation,withdrawal_id,qitem_id,outbox_id,action_json,stage,created_at) VALUES(?,?,?,?,?,?,'prepared',?)")
      .run(action.actionId,action.generation,action.withdrawalId,action.qitemId,action.outboxId,JSON.stringify(action),Date.now());
    return action;
  }).immediate();
}

export function finalizeElasticNativeWakeParent(db:Database.Database,outboxId:string):void {
  if(!db.inTransaction)deny();
  if(!db.prepare("SELECT 1 FROM sqlite_master WHERE name='elastic_native_wakes'").get())return;
  const row=z.object({parent_id:text.nullable()}).safeParse(db.prepare('SELECT parent_id FROM elastic_native_wakes WHERE outbox_id=?').get(outboxId));
  if(row.success && row.data.parent_id)db.prepare('UPDATE elastic_native_wakes SET parent_digest=? WHERE outbox_id=?').run(workerDigest(continuationQueue(db,row.data.parent_id)),outboxId);
}
