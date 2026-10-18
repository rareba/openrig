import { mkdtempSync,realpathSync,mkdirSync,writeFileSync,readFileSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LinuxWorkerLifecycle } from '../src/adapters/linux-worker-lifecycle.js';
import type { LinuxPrivilegedWorkerBoundary } from '../src/adapters/linux-privileged-worker-boundary.js';
import { continuationBootId } from '../src/adapters/elastic-owned-continuation.js';
import { azureWorkerId } from '../src/adapters/azure-elastic-fleet.js';
import { afterEach, describe, expect, it } from 'vitest';
import Database from 'better-sqlite3';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { ElasticWorkerControl, workerDigest } from '../src/domain/elastic-worker-control.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { EventBus } from '../src/domain/event-bus.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { assertContinuationAction, continuationQueue, issueWithdrawalAction, issueOwnedSuccessor, nativeContinuation, readContinuationAction, type WithdrawalRequest } from '../src/domain/elastic-continuation.js';
const dbs:Database.Database[]=[];const dirs:string[]=[];
afterEach(()=>{for(const db of dbs.splice(0))db.close();for(const d of dirs.splice(0))rmSync(d,{recursive:true,force:true});});
async function fixture(database=':memory:') {
  const db=new Database(database);dbs.push(db);migrate(db,ALL_MIGRATIONS);
  db.exec(`INSERT INTO rigs(id,name) VALUES('rig','rig');
    INSERT INTO nodes(id,rig_id,logical_id,runtime,cwd,model) VALUES('node','rig','owner','pi','/work','test/model');
    INSERT INTO sessions(id,node_id,session_name,resume_type,resume_token) VALUES('session','node','owner@rig','pi_session_file','/work/session.jsonl');
    INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES('binding','node','owner@rig','%1');
    INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('tenure','node',1,'occupant','fresh');`);
  const identity={workerId:azureWorkerId('/fixture','11111111-1111-4111-8111-111111111111'),vmId:'11111111-1111-4111-8111-111111111111',manifestDigest:'a'.repeat(64)};
  const control=new ElasticWorkerControl(db),queue=new QueueRepository(db,new EventBus(db));queue.attachOutbox(new OutboxHandler(db));
  control.enroll(identity);
  const question=await queue.create({sourceSession:'owner@rig',destinationSession:'human@kernel',body:'Decision',summary:'Decide',evidenceRef:'/proof',humanIntent:'decision',nudge:false});
  control.beginDrain(identity,'begin-1');
  const answer={qitemId:question.qitemId,requestRevision:question.humanRequestRevision!,actorSession:'human@kernel',replyId:'one',response:{kind:'custom' as const,text:'Continue exactly this task'}};
  const reply=queue.recordHumanResponse(answer),outboxId=`wake-intent-${reply.replyQitemId}`;
  const {native}=nativeContinuation(db,outboxId);
  const request:WithdrawalRequest={identity,generation:1,operationId:'begin-1',withdrawalId:'22222222-2222-4222-8222-222222222222',outboxId,queueDigest:native.queue_digest,
    target:{nodeId:'node',session:'owner@rig',occupant:'occupant',pane:'%1',conversation:'conversation',sessionFile:'/work/session.jsonl',stateRoot:'/work/state/pi',cwd:'/work',model:'test/model',launchId:'launch',process:{bootId:'boot',runnerPid:123,runnerStartTime:'100',childPid:124,childStartTime:'101'}}};
  const issue=()=>db.transaction(()=>issueWithdrawalAction(db,request,'existing-runner')).immediate();
  return {db,control,queue,identity,question,answer,reply,request,issue};
}
describe('native generation continuation custody',()=>{
  it('uses a genuine human response and exactly one native action, retaining both generations',async()=>{
    const f=await fixture();const action=f.issue();
    expect(f.queue.recordHumanResponse(f.answer)).toEqual(f.reply);
    expect(f.issue()).toEqual(action);
    expect(action).toMatchObject({generation:2,qitemId:f.reply.replyQitemId,parentId:f.question.qitemId,effect:{kind:'existing-runner'}});
    expect(f.db.prepare('SELECT COUNT(*) AS n FROM elastic_continuation_actions').get()).toEqual({n:1});
    expect(f.control.generation()).toMatchObject({purpose:'continuation',predecessor:1});
    expect(f.db.prepare('SELECT generation FROM elastic_worker_custody WHERE qitem_id=? ORDER BY generation').all(f.reply.replyQitemId)).toEqual([{generation:1},{generation:2}]);
    await expect(f.queue.create({sourceSession:'owner@rig',destinationSession:'owner@rig',body:'Unrelated root',nudge:false})).rejects.toThrow('elastic_worker_fenced');
  });
  it.each(['body','owner','parent','claim','node','occupant','conversation','generation','payload'] as const)('denies %s drift after action preparation',async kind=>{
    const f=await fixture(),action=f.issue();
    if(kind==='body')f.db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run('changed',action.qitemId);
    if(kind==='claim')f.db.prepare('UPDATE queue_items SET claimed_by_generation_uuid=? WHERE qitem_id=?').run('replacement',action.qitemId);
    if(kind==='payload')f.db.prepare('UPDATE elastic_continuation_actions SET action_json=? WHERE action_id=?').run(JSON.stringify({...action,payload:'changed',payloadDigest:workerDigest('changed')}),action.actionId);
    if(kind==='owner')f.db.prepare('UPDATE outbox_entries SET destination_session=? WHERE outbox_id=?').run('other@rig',action.outboxId);
    if(kind==='parent')f.db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run('changed',action.parentId);
    if(kind==='node')f.db.prepare("UPDATE nodes SET cwd='/other' WHERE id='node'").run();
    if(kind==='occupant')f.db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('next','node',2,'replacement','new')").run();
    if(kind==='conversation')f.db.prepare("UPDATE sessions SET resume_token='/work/other.jsonl'").run();
    if(kind==='generation')f.db.prepare('UPDATE elastic_worker_control SET generation=1 WHERE id=1').run();
    expect(()=>assertContinuationAction(f.db,action.actionId)).toThrow();
  });
  it('rejects an outbox-only forged activation and changed withdrawal selectors',async()=>{
    const f=await fixture();f.db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state,audit_pointer) VALUES('wake-intent-forged','human@kernel','owner@rig','forged','','pending',?)").run(f.reply.replyQitemId);
    expect(()=>nativeContinuation(f.db,'wake-intent-forged')).toThrow();
    f.issue();
    expect(()=>f.db.transaction(()=>issueWithdrawalAction(f.db,{...f.request,queueDigest:'b'.repeat(64)},'existing-runner')).immediate()).toThrow();
    expect(()=>f.db.transaction(()=>issueWithdrawalAction(f.db,{...f.request,withdrawalId:'33333333-3333-4333-8333-333333333333'},'existing-runner')).immediate()).toThrow();
  });
  it('cannot seal continuation, retire unproved work or reuse an old drain operation',async()=>{
    const f=await fixture(),action=f.issue();
    expect(()=>f.control.sealAdmission(f.identity,2)).toThrow();
    expect(()=>f.control.retireContinuation()).toThrow();
    expect(()=>f.control.beginDrain(f.identity,'begin-1')).toThrow();
    expect(readContinuationAction(f.db,action.actionId).stage).toBe('prepared');
  });
  it('derives successor authority only through actual native handoff ancestry',async()=>{
    const f=await fixture(),action=f.issue();
    // This is a domain issuance test; native runtime proof is exercised by the separate runner fixture.
    f.db.prepare("UPDATE elastic_continuation_actions SET stage='proved' WHERE action_id=?").run(action.actionId);
    const successor=await f.queue.handoffAndComplete({qitemId:action.qitemId,fromSession:'owner@rig',toSession:'owner@rig'});
    const outboxId=`wake-intent-${successor.created.qitemId}`;
    const next=issueOwnedSuccessor(f.db,outboxId,f.request.target,'resume-conversation');
    expect(next).toMatchObject({generation:2,parentId:action.qitemId,effect:{kind:'resume-conversation'}});
    expect(next.effect.kind==='resume-conversation'&&next.effect.launchId).toBeTruthy();
    expect(issueOwnedSuccessor(f.db,outboxId,f.request.target,'resume-conversation')).toEqual(next);
    expect(()=>issueOwnedSuccessor(f.db,outboxId,{...f.request.target,nodeId:'other'},'resume-conversation')).toThrow();
  });
  it('retains immutable old custody and enters a distinct next drain after proved changed disposition',async()=>{
    const f=await fixture(),action=f.issue();
    f.db.prepare("UPDATE elastic_continuation_actions SET stage='proved',proved_at=1 WHERE action_id=?").run(action.actionId);
    f.db.prepare('UPDATE elastic_worker_generations SET progress_at=1 WHERE generation=2').run();
    f.db.prepare("UPDATE queue_items SET state='done',ts_updated='fresh-disposition' WHERE qitem_id=?").run(action.qitemId);
    f.db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run(action.outboxId);
    f.control.retireContinuation();
    expect(f.control.beginDrain(f.identity,'begin-3')).toMatchObject({phase:'draining',generation:3,drainOperationId:'begin-3'});
    expect(f.db.prepare('SELECT generation,purpose,operation_id FROM elastic_worker_generations ORDER BY generation').all()).toEqual([
      {generation:1,purpose:'removal',operation_id:'begin-1'}, {generation:2,purpose:'continuation',operation_id:f.request.withdrawalId},{generation:3,purpose:'removal',operation_id:'begin-3'}]);
    expect(()=>assertContinuationAction(f.db,action.actionId)).toThrow();
    expect(f.db.prepare('SELECT COUNT(*) AS n FROM elastic_worker_custody WHERE generation=1').get()).toEqual({n:2});
  });
  it('retains sealed admission and refuses withdrawal before creating any new generation',async()=>{
    const f=await fixture();f.control.sealAdmission(f.identity,1);
    expect(()=>f.issue()).toThrow();expect(f.control.read()?.generation).toBe(1);
  });
});

describe('withdrawal publication crash recovery (native transactions; synthetic root publisher)',()=>{
  async function publication(){
    const root=realpathSync(mkdtempSync(join(tmpdir(),'continuation-publish-')));dirs.push(root);
    const controlDirectory=join(root,'control');mkdirSync(controlDirectory);
    const f=await fixture(join(root,'worker.sqlite')),sessionFile=join(root,'session.jsonl');
    writeFileSync(sessionFile,JSON.stringify({type:'session',id:'conversation'})+'\n');
    f.db.prepare('UPDATE sessions SET resume_token=?').run(sessionFile);
    f.request.target={...f.request.target,sessionFile,process:{bootId:continuationBootId(),runnerPid:2147483646,childPid:2147483645,runnerStartTime:'absent',childStartTime:'absent'}};
    let failAt:'transition'|'continuation'|null=null;
    const marker=join(root,'marker');
    const os={manifestDigest:f.identity.manifestDigest,instance:()=>f.identity,manifest:{resourceId:'/fixture',controlDirectory,linux:{stateRoot:f.request.target.stateRoot,seats:['owner@rig']}},
      publishAdmission:(state:unknown,purpose:string,operationId:string)=>{writeFileSync(marker,JSON.stringify({state,purpose,operationId}));if(purpose===failAt)throw Error('publication_crash');}} as unknown as LinuxPrivilegedWorkerBoundary;
    return {...f,root,controlDirectory,marker,os,lifecycle:new LinuxWorkerLifecycle(os,f.control),fail:(at:typeof failAt)=>failAt=at};
  }
  it.each(['transition','continuation'] as const)('recovers the same immutable request after %s publication failure',async point=>{
    const f=await publication();f.fail(point);
    expect(()=>f.lifecycle.withdraw(f.request)).toThrow('publication_crash');
    expect(f.control.read()?.phase).toBe('draining');
    expect(f.control.read()?.generation).toBe(point==='transition'?1:2);
    const intent=readFileSync(join(f.controlDirectory,`withdrawal-${f.request.withdrawalId}.json`),'utf8');
    expect(()=>f.lifecycle.withdraw({...f.request,queueDigest:'b'.repeat(64)})).toThrow();
    f.fail(null);const recovered=new LinuxWorkerLifecycle(f.os,f.control).withdraw(f.request);
    expect(f.lifecycle.withdraw(f.request)).toEqual(recovered);
    expect(f.db.prepare('SELECT COUNT(*) AS n FROM elastic_continuation_actions').get()).toEqual({n:1});
    expect(JSON.parse(readFileSync(f.marker,'utf8'))).toMatchObject({purpose:'continuation',state:{generation:2,phase:'draining'}});
    expect(readFileSync(join(f.controlDirectory,`withdrawal-${f.request.withdrawalId}.json`),'utf8')).toBe(intent);
  });
  it.each(['quiesce-intent.json','quiesced.json'])('denies any persisted %s before creating withdrawal authority',async name=>{
    const f=await publication();writeFileSync(join(f.controlDirectory,name),'{}');
    expect(()=>f.lifecycle.withdraw(f.request)).toThrow();
    expect(f.control.read()?.generation).toBe(1);expect(f.db.prepare('SELECT COUNT(*) AS n FROM elastic_continuation_actions').get()).toEqual({n:0});
  });
});
