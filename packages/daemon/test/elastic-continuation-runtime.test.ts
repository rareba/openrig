import { afterEach, describe, expect, it } from 'vitest';
import Database from 'better-sqlite3';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { z } from 'zod';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { EventBus } from '../src/domain/event-bus.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { ElasticWorkerControl } from '../src/domain/elastic-worker-control.js';
import { continuationProcessSchema, continuationTransition, issueWithdrawalAction, issueOwnedSuccessor, nativeContinuation, readContinuationAction, type ContinuationAction } from '../src/domain/elastic-continuation.js';
import { continuationManifest, observeContinuationTarget, reconcileOwnedContinuation, spawnOwnedContinuation, validateWithdrawalTarget, verifyContinuationProcess } from '../src/adapters/elastic-owned-continuation.js';
import { assertElasticEffectAllowed, assertElasticContinuationAllowed, assertElasticTmuxEffectAllowed, withElasticContinuationTransport } from '../src/domain/elastic-worker-guard.js';
import { SeatDeliveryGuard } from '../src/domain/seat-delivery-guard.js';
import { TmuxAdapter } from '../src/adapters/tmux.js';
import { parseLinuxManifest, pinDigest, procIdentity } from '../src/adapters/linux-privileged-worker-boundary.js';
import { piSeatPaths } from '../src/adapters/pi-runner-protocol.js';
import { shellQuote } from '../src/adapters/shell-quote.js';
const root=process.env.OPENRIG_CONTINUATION_TEST_ROOT;
const original={...process.env};
let cleanup:()=>void=()=>{};
afterEach(()=>{cleanup();cleanup=()=>{};for(const key of Object.keys(process.env))if(!(key in original))delete process.env[key];Object.assign(process.env,original);});
async function until(fn:()=>boolean,label:string):Promise<void>{for(let i=0;i<150;i++){if(fn())return;await new Promise(r=>setTimeout(r,40));}throw Error(`timeout: ${label}`);}
/** Real SQLite + native queue APIs + genuine UID-owned runner/child processes + private tmux.
 * Root issuance and systemd cgroup membership deliberately are not claimed by this source fixture.
 */
describe.skipIf(process.platform!=='linux'||!root)('owned continuation across the actual runner process',()=>{
  async function fixture(activationResume=false){
    const fixed=z.object({work:z.string(),uid:z.number(),gid:z.number(),runner:z.string(),identity:z.object({workerId:z.string(),vmId:z.string(),manifestDigest:z.string()})}).parse(JSON.parse(readFileSync(join(root!,'fixture.json'),'utf8')));
    expect(process.getuid!()).toBe(fixed.uid);expect(process.getgid!()).toBe(fixed.gid);
    const manifest=parseLinuxManifest(readFileSync(join(root!,'manifest.json'),'utf8'));
    for(const pin of manifest.pins)expect(pinDigest(pin.path,pin.kind)).toBe(pin.sha256);
    rmSync(fixed.work,{recursive:true,force:true});mkdirSync(manifest.linux.home,{recursive:true});mkdirSync(manifest.linux.temp,{recursive:true});
    const sessionFile=join(manifest.linux.home,'session.jsonl');writeFileSync(sessionFile,JSON.stringify({type:'session',id:'conversation'})+'\n');
    Object.assign(process.env,{OPENRIG_DB:manifest.linux.database,OPENRIG_ELASTIC_GUARD:manifest.linux.guard,OPENRIG_ELASTIC_MANIFEST:join(root!,'manifest.json'),OPENRIG_ELASTIC_MANIFEST_DIGEST:fixed.identity.manifestDigest,OPENRIG_ELASTIC_PI:manifest.linux.pi,OPENRIG_HOME:manifest.linux.home});
    const socket=join(fixed.work,'tmux.sock');
    const tm=(args:string[])=>execFileSync(manifest.linux.tmux,['-S',socket,...args],{encoding:'utf8',env:process.env,timeout:5000});
    tm(['-f','/dev/null','new-session','-d','-s','owner@rig','-c',manifest.linux.home,manifest.linux.shell]);
    const pane=tm(['display-message','-p','-t','=owner@rig:','#{pane_id}']).trim();
    const baseline=[manifest.linux.node,fixed.runner,'--session-name','owner@rig','--state-root',manifest.linux.stateRoot,'--cwd',manifest.linux.home,'--launch-id','initial-launch','--no-approve','--session',sessionFile].map(shellQuote).join(' ');
    tm(['send-keys','-t',pane,'-l','--',baseline]);tm(['send-keys','-t',pane,'Enter']);
    const sidecar=piSeatPaths(manifest.linux.stateRoot,'owner@rig').runnerStatePath;
    await until(()=>{try{return JSON.parse(readFileSync(sidecar,'utf8')).ready;}catch{return false;}},'pre-existing runner ready');
    const ids=continuationProcessSchema.parse(JSON.parse(readFileSync(sidecar,'utf8')).processIdentity);
    const db=new Database(manifest.linux.database);db.pragma('journal_mode=WAL');db.pragma('busy_timeout=3000');migrate(db,ALL_MIGRATIONS);
    cleanup=()=>{try{tm(['kill-server']);}catch{}db.close();};
    db.exec(`INSERT INTO rigs(id,name) VALUES('rig','rig'); INSERT INTO nodes(id,rig_id,logical_id,runtime,cwd) VALUES('node','rig','owner','pi','${manifest.linux.home}');
      INSERT INTO sessions(id,node_id,session_name,resume_type,resume_token) VALUES('session','node','owner@rig','pi_session_file','${sessionFile}');
      INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES('binding','node','owner@rig','${pane}');
      INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('tenure','node',1,'occupant','fresh');`);
    const control=new ElasticWorkerControl(db);control.enroll(fixed.identity);
    const queue=new QueueRepository(db,new EventBus(db));queue.attachOutbox(new OutboxHandler(db));
    const question=activationResume ? queue.stageHumanResponseReturn({sourceSession:'human@kernel',destinationSession:'owner@rig',body:'Committed human answer',qitemId:'staged-return',tags:['response:fingerprint']}) : await queue.create({sourceSession:'owner@rig',destinationSession:'human@kernel',body:'Decision',summary:'Decide',evidenceRef:'/synthetic/proof',humanIntent:'decision',nudge:false});
    control.beginDrain(fixed.identity,'begin-1');
    const answer=activationResume ? {replyQitemId:queue.activateHumanResponseReturn({qitemId:question.qitemId,actorSession:'human@kernel',fingerprint:'fingerprint'}).qitemId} : queue.recordHumanResponse({qitemId:question.qitemId,requestRevision:question.humanRequestRevision!,actorSession:'human@kernel',replyId:'reply',response:{kind:'custom',text:'Continue the existing task'}});
    const outboxId=`wake-intent-${answer.replyQitemId}`,target=observeContinuationTarget(db,outboxId,manifest),native=nativeContinuation(db,outboxId).native;
    if(activationResume){process.kill(ids.runnerPid,'SIGTERM');await until(()=>{try{verifyContinuationProcess(target,'absent',manifest);return true;}catch{return false;}},'initial processes gone');}
    if(activationResume)expect(validateWithdrawalTarget(db,outboxId,target,manifest)).toBe('resume-conversation');
    const action=db.transaction(()=>issueWithdrawalAction(db,{identity:fixed.identity,generation:1,operationId:'begin-1',withdrawalId:'22222222-2222-4222-8222-222222222222',outboxId,queueDigest:native.queue_digest,target},activationResume?'resume-conversation':'existing-runner')).immediate();
    const typed=new TmuxAdapter(async()=>{throw Error('shell transport forbidden');},undefined,async argv=>{assertElasticTmuxEffectAllowed(argv);return tm(argv.slice(1));});
    typed.deliveryGuard=new SeatDeliveryGuard(db,()=>({nodeId:'node',session:'owner@rig',occupant:'occupant',pane}));
    const messages=()=>readFileSync(sessionFile,'utf8').trim().split('\n').slice(1).map(line=>JSON.parse(line));
    const prove=async(a:ContinuationAction)=>{await until(()=>reconcileOwnedContinuation(db,a.actionId)==='proved',`native consumption ${a.actionId}`);};
    return {fixed,manifest,db,queue,control,typed,action,ids,target,tm,pane,sidecar,sessionFile,messages,prove,baseline};
  }
  it('consumes the exact native human wake once, then resumes that conversation for its native successor',async()=>{
    const f=await fixture();expect(procIdentity(f.ids.runnerPid).uid).toBe(501);expect(procIdentity(f.ids.childPid).ppid).toBe(f.ids.runnerPid);
    expect(()=>assertElasticEffectAllowed(f.db)).toThrow();
    await expect(withElasticContinuationTransport(f.action.actionId,['tmux','send-keys','-t',f.pane,'-l','--','forged'],async()=>assertElasticTmuxEffectAllowed(['tmux','send-keys','-t',f.pane,'-l','--','forged']))).rejects.toThrow();
    expect(await f.typed.sendOwnedContinuation(f.action.actionId)).toBe('issued');await f.prove(f.action);
    expect(f.messages().map(m=>m.message.content[0].text)).toEqual([f.action.payload]);
    expect(await f.typed.sendOwnedContinuation(f.action.actionId)).toBe('proved');
    f.tm(['send-keys','-t',f.pane,'-l','--','unattributed raw prompt']);f.tm(['send-keys','-t',f.pane,'Enter']);
    f.tm(['send-keys','-t',f.pane,'-l','--','@openrig-continuation:'+f.action.actionId]);f.tm(['send-keys','-t',f.pane,'Enter']);
    await new Promise(r=>setTimeout(r,100));expect(f.messages()).toHaveLength(1);
    const handoff=await f.queue.handoffAndComplete({qitemId:f.action.qitemId,fromSession:'owner@rig',toSession:'owner@rig'});
    process.kill(f.ids.runnerPid,'SIGTERM');await until(()=>{try{verifyContinuationProcess(f.target,'absent',f.manifest);return true;}catch{return false;}},'old processes gone');
    const outboxId=`wake-intent-${handoff.created.qitemId}`;
    const successor=issueOwnedSuccessor(f.db,outboxId,observeContinuationTarget(f.db,outboxId,f.manifest),'resume-conversation');
    expect(await f.typed.sendOwnedContinuation(successor.actionId)).toBe('issued');await f.prove(successor);
    expect(f.messages().map(m=>m.message.content[0].text)).toEqual([f.action.payload,successor.payload]);
    const rebound=readContinuationAction(f.db,successor.actionId).runner!;
    expect(readFileSync(`/proc/${rebound.runnerPid}/environ`,'utf8')).toContain('OPENRIG_OCCUPANT_GENERATION=occupant\0');
    expect(readFileSync(`/proc/${rebound.runnerPid}/environ`,'utf8')).toContain('OPENRIG_SESSION_NAME=owner@rig\0');
    expect(rebound.runnerPid).not.toBe(f.ids.runnerPid);expect(procIdentity(rebound.childPid).ppid).toBe(rebound.runnerPid);
    expect(JSON.parse(readFileSync(f.sidecar,'utf8')).sessionId).toBe('conversation');
    expect(JSON.parse(readFileSync(f.sidecar,'utf8')).sessionFile).toBe(f.sessionFile);
    expect(await f.typed.sendOwnedContinuation(successor.actionId)).toBe('proved');
    f.queue.update({qitemId:successor.qitemId,actorSession:'owner@rig',state:'done',closureReason:'no-follow-on',resolution:'Synthetic continuation complete',transitionNote:'Completed in the existing conversation'});
    expect(f.control.ledger().clean).toBe(true);f.control.retireContinuation();
    expect(f.control.beginDrain(f.fixed.identity,'distinct-begin-3')).toMatchObject({phase:'draining',generation:3});
    expect(()=>assertElasticContinuationAllowed(successor.actionId,f.db)).toThrow();
    expect(f.db.prepare('SELECT generation,purpose FROM elastic_worker_generations ORDER BY generation').all()).toEqual([{generation:1,purpose:'removal'},{generation:2,purpose:'continuation'},{generation:3,purpose:'removal'}]);
  },20000);
  it.each(['before-claim','after-claim'] as const)('denies a replacement native runner %s without spawning another child',async point=>{
    const f=await fixture(true);
    const oldSidecar=readFileSync(f.sidecar,'utf8');
    f.tm(['new-window','-d','-t','=owner@rig:','-n','replacement',f.baseline.replace('initial-launch','replacement-launch')]);
    await until(()=>{try{const s=JSON.parse(readFileSync(f.sidecar,'utf8'));return s.ready&&s.launchId==='replacement-launch';}catch{return false;}},'replacement native runner');
    const replacement=readFileSync(f.sidecar,'utf8');
    const ids=continuationProcessSchema.parse(JSON.parse(replacement).processIdentity);
    expect(()=>process.kill(ids.runnerPid,0)).not.toThrow();expect(()=>process.kill(ids.childPid,0)).not.toThrow();
    if(point==='after-claim'){
      writeFileSync(f.sidecar,oldSidecar);
      f.db.function('replace_native_sidecar',()=>{writeFileSync(f.sidecar,replacement);return 1;});
      f.db.exec("CREATE TRIGGER replace_at_spawn AFTER UPDATE OF stage ON elastic_continuation_actions WHEN NEW.stage='spawn-issued' BEGIN SELECT replace_native_sidecar(); END");
    }
    f.db.transaction(()=>continuationTransition(f.db,f.action.actionId,'prepared','transport-issued')).immediate();
    expect(f.action.effect.kind).toBe('resume-conversation');
    if(f.action.effect.kind!=='resume-conversation')throw Error('fixture requires resume');
    const launchId=f.action.effect.launchId;
    let spawned:ReturnType<typeof spawnOwnedContinuation>|undefined;
    try {
      expect(()=>{spawned=spawnOwnedContinuation(f.db,f.action.actionId,{sessionName:f.target.session,stateRoot:f.target.stateRoot,cwd:f.target.cwd,launchId,sessionFile:f.target.sessionFile,model:f.target.model??undefined,trust:'no-approve'},process.env);}).toThrow('elastic_continuation_denied');
      expect(spawned).toBeUndefined();
      expect(readContinuationAction(f.db,f.action.actionId)).toMatchObject({stage:point==='before-claim'?'transport-issued':'spawn-issued',runner:null});
      expect(f.messages()).toEqual([]);
      expect(f.db.prepare('SELECT COUNT(*) AS n FROM elastic_continuation_actions').get()).toEqual({n:1});
      expect(JSON.parse(readFileSync(f.sidecar,'utf8')).processIdentity).toEqual(ids);
    } finally {spawned?.kill();}
  },20000);
  it('derives native staged activation and refuses missing-conversation resume without a fresh child',async()=>{
    const f=await fixture(true);expect(f.action.parentId).toBeNull();
    expect(f.action.effect.kind).toBe('resume-conversation');unlinkSync(f.sessionFile);
    expect(await f.typed.sendOwnedContinuation(f.action.actionId)).toBe('issued');
    await until(()=>f.tm(['capture-pane','-p','-t',f.pane,'-S','-100']).includes('ENOENT'),'missing session rejected');
    expect(readContinuationAction(f.db,f.action.actionId)).toMatchObject({stage:'transport-issued',runner:null});
    expect(existsSync(f.sessionFile)).toBe(false);expect(reconcileOwnedContinuation(f.db,f.action.actionId)).toBe('unknown');
    expect(await f.typed.sendOwnedContinuation(f.action.actionId)).toBe('unknown');
  },20000);
  it('retains lost acknowledgement without resending, and denies stale root generations and PID identity',async()=>{
    const f=await fixture();
    for(const marker of ['old','transition','sealed']){process.env.OPENRIG_ELASTIC_GUARD=join(root!,marker+'.json');expect(()=>assertElasticContinuationAllowed(f.action.actionId,f.db)).toThrow();}
    process.env.OPENRIG_ELASTIC_GUARD=f.manifest.linux.guard;
    expect(()=>verifyContinuationProcess({...f.target,process:{...f.target.process,runnerStartTime:'not-this-process'}},'live',f.manifest)).toThrow();
    writeFileSync(f.sessionFile+'.drop-ack','1');expect(await f.typed.sendOwnedContinuation(f.action.actionId)).toBe('issued');
    await until(()=>f.messages().length===1,'RPC effect');
    expect(reconcileOwnedContinuation(f.db,f.action.actionId)).toBe('unknown');
    expect(await f.typed.sendOwnedContinuation(f.action.actionId)).toBe('unknown');
    f.tm(['send-keys','-t',f.pane,'-l','--','@openrig-continuation:'+f.action.actionId]);f.tm(['send-keys','-t',f.pane,'Enter']);
    await new Promise(r=>setTimeout(r,100));expect(f.messages()).toHaveLength(1);
    expect(()=>f.control.retireContinuation()).toThrow();expect(()=>f.control.beginDrain(f.fixed.identity,'begin-next')).toThrow();
  },20000);
});
