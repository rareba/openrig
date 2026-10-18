import Database from 'better-sqlite3';
import { execFileSync, spawn } from 'node:child_process';
import { readFileSync, realpathSync, lstatSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { z } from 'zod';
import { assertElasticContinuationAllowed } from '../domain/elastic-worker-guard.js';
import { assertContinuationTarget, continuationProcessSchema, continuationTargetSchema, continuationTransition, nativeContinuation, readContinuationAction,
  type ContinuationAction, type ContinuationTarget } from '../domain/elastic-continuation.js';
import { workerDigest } from '../domain/elastic-worker-control.js';
import { buildPiChildArgs, parsePiRunnerState, piSeatPaths } from './pi-runner-protocol.js';
import { assertRootPath, parseLinuxManifest, procIdentity, type LinuxWorkerManifest } from './linux-privileged-worker-boundary.js';
import { noFollowRead, sha256 } from './elastic-worker-checkpoint.js';
import { shellQuote } from './shell-quote.js';

export const CONTINUATION_FRAME='@openrig-continuation:';
function deny():never {throw Error('elastic_continuation_denied');}
export function continuationBootId():string {
  return process.platform==='linux' ? readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim()
    : execFileSync('/usr/sbin/sysctl',['-n','kern.boottime'],{encoding:'utf8',timeout:2000}).trim();
}
function start(pid:number):string|null {
  try{process.kill(pid,0);}catch(error){if(z.object({code:z.literal('ESRCH')}).safeParse(error).success)return null;throw error;}
  if(process.platform==='linux') {const stat=readFileSync(`/proc/${pid}/stat`,'utf8');return stat.slice(stat.lastIndexOf(')')+2).split(' ')[19]??deny();}
  const value=execFileSync('/bin/ps',['-p',String(pid),'-o','lstart='],{encoding:'utf8',env:{LC_ALL:'C',PATH:'/usr/bin:/bin'},timeout:2000}).trim();
  return value||deny();
}
export function currentContinuationProcess(childPid:number) {
  return continuationProcessSchema.parse({bootId:continuationBootId(),runnerPid:process.pid,runnerStartTime:start(process.pid),childPid,childStartTime:start(childPid)});
}
export function continuationManifest():LinuxWorkerManifest {
  const path=process.env.OPENRIG_ELASTIC_MANIFEST??deny(); assertRootPath(path); const bytes=noFollowRead(path,65536);
  if(sha256(bytes)!==process.env.OPENRIG_ELASTIC_MANIFEST_DIGEST) deny();
  const manifest=parseLinuxManifest(bytes.toString());
  if(realpathSync(manifest.linux.database)!==realpathSync(process.env.OPENRIG_DB??'')) deny();
  const db=new Database(manifest.linux.database,{readonly:true,fileMustExist:true});
  try { const state=z.object({manifest_digest:z.string()}).parse(db.prepare('SELECT manifest_digest FROM elastic_worker_control WHERE id=1').get());
    if(state.manifest_digest!==sha256(bytes))deny();
  }finally{db.close();}
  return manifest;
}
export function verifyContinuationProcess(target:ContinuationTarget,mode:'live'|'absent',manifest?:LinuxWorkerManifest):void {
  const p=target.process;
  if(continuationBootId()!==p.bootId) deny();
  const runner=start(p.runnerPid),child=start(p.childPid);
  if(mode==='absent') {if(runner!==null||child!==null)deny();return;}
  if(runner!==p.runnerStartTime || child!==p.childStartTime) deny();
  if(manifest && process.platform==='linux') {
    const r=procIdentity(p.runnerPid),c=procIdentity(p.childPid);
    if(r.uid!==manifest.uid || c.uid!==manifest.uid || c.ppid!==r.pid || r.executable!==manifest.linux.node || c.executable!==manifest.linux.node
      || r.argv[1]!==join(dirname(manifest.linux.helper),'pi-runner.js') || c.argv[1]!==manifest.linux.pi
      || r.argv[r.argv.indexOf('--launch-id')+1]!==target.launchId) deny();
  }
}
function sessionHeader(file:string,conversation:string):Buffer {
  const bytes=noFollowRead(file,64*1024*1024),line=bytes.toString().split('\n')[0];
  const header=z.object({type:z.literal('session'),id:z.string()}).parse(JSON.parse(line??''));
  if(header.id!==conversation)deny();return bytes;
}
export function observeContinuationTarget(db:Database.Database,outboxId:string,manifest:LinuxWorkerManifest):ContinuationTarget {
  const {queue}=nativeContinuation(db,outboxId);
  const row=z.object({nodeId:z.string(),session:z.string(),occupant:z.string(),pane:z.string(),sessionFile:z.string(),cwd:z.string(),model:z.string().nullable()}).parse(
    db.prepare(`SELECT n.id AS nodeId,s.session_name AS session,b.tmux_pane AS pane,s.resume_token AS sessionFile,n.cwd,n.model,
      (SELECT generation_uuid FROM occupant_tenures WHERE node_id=n.id ORDER BY generation_ordinal DESC LIMIT 1) AS occupant
      FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN bindings b ON b.node_id=n.id
      WHERE s.session_name=? AND s.resume_type='pi_session_file' AND n.runtime='pi' ORDER BY s.id DESC LIMIT 1`).get(queue.destination_session));
  if(!manifest.linux.seats.includes(row.session))deny();
  const sidecar=parsePiRunnerState(noFollowRead(piSeatPaths(manifest.linux.stateRoot,row.session).runnerStatePath,65536).toString());
  const target=continuationTargetSchema.parse({...row,stateRoot:manifest.linux.stateRoot,launchId:sidecar?.launchId,conversation:sidecar?.sessionId,process:sidecar?.processIdentity});
  if(sidecar?.sessionFile!==target.sessionFile)deny();sessionHeader(target.sessionFile,target.conversation);
  return target;
}
export function validateWithdrawalTarget(db:Database.Database,target:ContinuationTarget,manifest:LinuxWorkerManifest):'existing-runner'|'resume-conversation' {
  assertContinuationTarget(db,target);
  if(target.stateRoot!==manifest.linux.stateRoot || !manifest.linux.seats.includes(target.session))deny();
  sessionHeader(target.sessionFile,target.conversation);
  const runner=start(target.process.runnerPid),child=start(target.process.childPid);
  if(runner===null&&child===null) {verifyContinuationProcess(target,'absent',manifest);return 'resume-conversation';}
  verifyContinuationProcess(target,'live',manifest);
  if(process.platform==='linux' && (procIdentity(target.process.runnerPid).cgroup!==`/system.slice/${manifest.unit}` || procIdentity(target.process.childPid).cgroup!==`/system.slice/${manifest.unit}`))deny();
  return 'existing-runner';
}
export function continuationTransportPlan(action:ContinuationAction,manifest:LinuxWorkerManifest):string {
  if(action.effect.kind==='existing-runner')return CONTINUATION_FRAME+action.actionId;
  const t=action.target;
  const identity=`OPENRIG_NODE_ID=${shellQuote(t.nodeId)} OPENRIG_OCCUPANT_GENERATION=${shellQuote(t.occupant)} OPENRIG_SESSION_NAME=${shellQuote(t.session)} `;
  return identity+[manifest.linux.node,join(dirname(manifest.linux.helper),'pi-runner.js'),'--session-name',t.session,'--state-root',t.stateRoot,
    '--cwd',t.cwd,'--launch-id',action.effect.launchId,'--no-approve','--session',t.sessionFile,'--elastic-continuation',action.actionId,
    ...(t.model?['--model',t.model]:[])].map(shellQuote).join(' ');
}
/** Called in the new runner immediately before the exact --session child spawn. */
export function claimContinuationSpawn(db:Database.Database,actionId:string,argv:{sessionName:string;stateRoot:string;cwd:string;launchId:string;sessionFile?:string;forkRef?:string;model?:string;trust:string}):void {
  db.transaction(()=>{
    const {action,stage}=assertElasticContinuationAllowed(actionId,db), t=action.target;
    if(action.effect.kind!=='resume-conversation' || stage!=='transport-issued' || argv.sessionName!==t.session || argv.stateRoot!==t.stateRoot
      || argv.cwd!==t.cwd || argv.launchId!==action.effect.launchId || argv.sessionFile!==t.sessionFile || argv.forkRef || (argv.model??null)!==t.model || argv.trust!=='no-approve')deny();
    verifyContinuationProcess(t,'absent',continuationManifest());sessionHeader(t.sessionFile,t.conversation);
    continuationTransition(db,actionId,'transport-issued','spawn-issued');
  }).immediate();
}
/** Persist before spawn, then hold native state stable through the concrete process effect. */
export function spawnOwnedContinuation(db:Database.Database,actionId:string,args:Parameters<typeof claimContinuationSpawn>[2],env:NodeJS.ProcessEnv) {
  claimContinuationSpawn(db,actionId,args);
  return db.transaction(()=>{
    const row=assertElasticContinuationAllowed(actionId,db),a=row.action,t=a.target,manifest=continuationManifest();
    if(a.effect.kind!=='resume-conversation'||row.stage!=='spawn-issued'||row.runner)deny();
    verifyContinuationProcess(t,'absent',manifest);sessionHeader(t.sessionFile,t.conversation);
    if(realpathSync(process.execPath)!==manifest.linux.node)deny();
    const child=spawn(manifest.linux.node,[manifest.linux.pi,'--no-extensions',...buildPiChildArgs({sessionsDir:piSeatPaths(t.stateRoot,t.session).sessionsDir,
      sessionName:t.session,model:t.model??undefined,trust:'no-approve',sessionFile:t.sessionFile,runtime:'pi'})],
      {cwd:t.cwd,env,stdio:['pipe','pipe','pipe']});
    if(child.pid)bindContinuationRunner(db,actionId,currentContinuationProcess(child.pid));
    return child;
  }).immediate();
}
export function bindContinuationRunner(db:Database.Database,actionId:string,processIdentity:z.infer<typeof continuationProcessSchema>):void {
  db.transaction(()=>{
    const row=assertElasticContinuationAllowed(actionId,db);
    if(row.action.effect.kind!=='resume-conversation'||row.stage!=='spawn-issued'||row.runner)deny();
    if(processIdentity.runnerPid!==process.pid || workerDigest(currentContinuationProcess(processIdentity.childPid))!==workerDigest(processIdentity))deny();
    db.prepare('UPDATE elastic_continuation_actions SET runner_json=? WHERE action_id=? AND runner_json IS NULL').run(JSON.stringify(processIdentity),actionId);
  }).immediate();
}
const receiptSchema=z.object({prefixBytes:z.number().int().nonnegative(),prefixDigest:z.string(),ack:z.boolean(),entryId:z.string().optional()}).strict();
/** The transaction extends through the synchronous RPC write, never through an await. */
export function writeOwnedContinuation(actionId:string,identity:{sessionName:string;nodeId?:string;generation?:string;launchId?:string},conversation:{sessionFile?:string;sessionId?:string},
  processIdentity:z.infer<typeof continuationProcessSchema>,send:(command:Record<string,unknown>)=>void):boolean {
  const db=new Database(process.env.OPENRIG_DB??'',{fileMustExist:true});
  try {const prepared=db.transaction(()=>{
    const row=assertElasticContinuationAllowed(actionId,db),a=row.action,t=a.target;
    if(row.stage==='input-issued'||row.stage==='proved')return false;
    const expected=a.effect.kind==='existing-runner'?t.process:row.runner;
    const launch=a.effect.kind==='existing-runner'?t.launchId:a.effect.launchId;
    if(!expected || identity.sessionName!==t.session || identity.nodeId!==t.nodeId || identity.generation!==t.occupant || identity.launchId!==launch
      || conversation.sessionFile!==t.sessionFile || conversation.sessionId!==t.conversation || workerDigest(expected)!==workerDigest(processIdentity)
      || processIdentity.runnerPid!==process.pid)deny();
    verifyContinuationProcess({...t,launchId:launch,process:expected},'live',continuationManifest());
    const required=a.effect.kind==='existing-runner'?'transport-issued':'spawn-issued';
    if(row.stage!==required)deny();
    const bytes=sessionHeader(t.sessionFile,t.conversation);
    db.prepare('UPDATE elastic_continuation_actions SET receipt_json=? WHERE action_id=?').run(JSON.stringify({prefixBytes:bytes.length,prefixDigest:sha256(bytes),ack:false}),actionId);
    continuationTransition(db,actionId,required,'input-issued');
    return true;
  }).immediate();
    if(!prepared)return false;
    db.transaction(()=>{
      const row=assertElasticContinuationAllowed(actionId,db),a=row.action;
      if(row.stage!=='input-issued')deny();
      const expected=a.effect.kind==='existing-runner'?a.target.process:row.runner;
      if(!expected||workerDigest(currentContinuationProcess(expected.childPid))!==workerDigest(expected))deny();
      verifyContinuationProcess({...a.target,launchId:a.effect.kind==='existing-runner'?a.target.launchId:a.effect.launchId,process:expected},'live',continuationManifest());
      send({type:'prompt',id:`elastic:${actionId}`,message:a.payload});
    }).immediate();
    return true;
  }finally{db.close();}
}
export function acknowledgeOwnedContinuation(actionId:string,reply:Record<string,unknown>):void {
  if(reply.type!=='response'||reply.id!==`elastic:${actionId}`||reply.command!=='prompt'||reply.success!==true)return;
  const db=new Database(process.env.OPENRIG_DB??'',{fileMustExist:true});
  try {db.transaction(()=>{
    const row=readContinuationAction(db,actionId);
    assertContinuationTarget(db,row.action.target);
    if(row.stage!=='input-issued'||!row.receipt) return;
    const p=row.action.effect.kind==='existing-runner'?row.action.target.process:row.runner;
    if(!p||p.runnerPid!==process.pid||start(process.pid)!==p.runnerStartTime)deny();
    const receipt=receiptSchema.parse(JSON.parse(row.receipt));
    db.prepare('UPDATE elastic_continuation_actions SET receipt_json=? WHERE action_id=?').run(JSON.stringify({...receipt,ack:true}),actionId);
  }).immediate();}finally{db.close();}
}
export function reconcileOwnedContinuation(db:Database.Database,actionId:string):'proved'|'unknown' {
  return db.transaction(()=>{
    const row=readContinuationAction(db,actionId);if(row.stage==='proved')return 'proved';
    if(row.stage!=='input-issued'||!row.receipt)return 'unknown';
    const a=row.action,receipt=receiptSchema.parse(JSON.parse(row.receipt));
    if(!receipt.ack)return 'unknown';
    // The queue may already be completed. The effect receipt instead binds its exact native conversation and append-only prefix.
    assertContinuationTarget(db,a.target);
    const bytes=sessionHeader(a.target.sessionFile,a.target.conversation);
    if(bytes.length<=receipt.prefixBytes||sha256(bytes.subarray(0,receipt.prefixBytes))!==receipt.prefixDigest)return 'unknown';
    const entries=bytes.subarray(receipt.prefixBytes).toString().split('\n').filter(Boolean);
    for(const raw of entries){
      const parsed=z.object({type:z.literal('message'),id:z.string(),message:z.object({role:z.literal('user'),content:z.union([z.string(),z.array(z.object({type:z.literal('text'),text:z.string()}))])})}).safeParse(JSON.parse(raw));
      if(!parsed.success)continue;
      const content=parsed.data.message.content;
      const text=typeof content==='string'?content:content.map(x=>x.text).join('');
      if(text!==a.payload)continue;
      db.prepare("UPDATE elastic_continuation_actions SET stage='proved',receipt_json=?,proved_at=? WHERE action_id=? AND stage='input-issued'").run(JSON.stringify({...receipt,entryId:parsed.data.id}),Date.now(),actionId);
      db.prepare('UPDATE elastic_worker_generations SET progress_at=? WHERE generation=?').run(Date.now(),a.generation);
      db.prepare("UPDATE outbox_entries SET delivery_state='delivered',delivered_at=? WHERE outbox_id=? AND delivery_state IN ('pending','retained')").run(new Date().toISOString(),a.outboxId);
      return 'proved';
    }
    return 'unknown';
  }).immediate();
}
