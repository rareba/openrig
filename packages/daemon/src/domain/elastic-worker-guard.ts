import { continuationManifest, continuationTransportPlan, verifyContinuationProcess } from '../adapters/elastic-owned-continuation.js';
import { AsyncLocalStorage } from 'node:async_hooks';
import { assertContinuationAction } from './elastic-continuation.js';
import { workerDigest } from './elastic-worker-control.js';
import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync, realpathSync } from "node:fs";
import Database from "better-sqlite3";
/** Reading/starting the daemon is distinct from permission to write to a seat. */
/** dbPath must be the database the daemon will actually open; OPENRIG_DB is only the fallback. */
export function assertElasticUnitStartAllowed(db?: Database.Database, dbPath?: string): void {
  readAdmission(db, dbPath);
}
function readAdmission(db?: Database.Database, dbPath?: string) {
  const path = process.env.OPENRIG_ELASTIC_GUARD;
  if (path === undefined) {
    if (existsSync('/etc/openrig-elastic/manifest.json')) throw Error('elastic_worker_effect_denied');
    return null;
  }
  let own:Database.Database|null=null;
  try {
    if(!db)own=new Database(dbPath ?? process.env.OPENRIG_DB ?? '', {readonly:true,fileMustExist:true});
    const st=lstatSync(path);
    if(realpathSync(path)!==path || !st.isFile() || st.uid!==0 || (st.mode&0o022)!==0 || st.size>4096) throw Error();
    const v=JSON.parse(readFileSync(path,'utf8'));
    const connection=db??own!;
    const state=connection.prepare('SELECT worker_id,vm_id,manifest_digest,phase,generation FROM elastic_worker_control WHERE id=1').get() as Record<string,unknown>|undefined;
    const generation=connection.prepare('SELECT purpose,operation_id,withdrawal_id FROM elastic_worker_generations WHERE generation=?').get(state?.generation??-1) as Record<string,unknown>|undefined;
    if(!state || v.version!==1 || !['ready','draining'].includes(v.phase) || state.phase!==v.phase
      || state.worker_id!==v.workerId || state.vm_id!==v.vmId || state.manifest_digest!==v.manifestDigest
      || (state.generation!==0 && (v.generation!==state.generation || v.purpose!==generation?.purpose || v.operationId!==generation?.operation_id))) throw Error();
    return state;
  } catch {throw Error('elastic_worker_effect_denied');}
  finally {own?.close();}
}
export function assertElasticEffectAllowed(db?: Database.Database): void {
  const state=readAdmission(db);
  if(state && state.phase!=='ready') throw Error('elastic_worker_effect_denied');
}
export function assertElasticContinuationAllowed(actionId:string, db:Database.Database) {
  if(!readAdmission(db)) throw Error('elastic_continuation_denied');
  return assertContinuationAction(db,actionId);
}
const transportScope=new AsyncLocalStorage<{actionId:string;argvDigest:string}>();
/** An exact command capability, revalidated by every concrete transport adapter. */
export function withElasticContinuationTransport<T>(actionId:string,argv:readonly string[],effect:()=>Promise<T>):Promise<T> {
  return transportScope.run({actionId,argvDigest:workerDigest(argv)},effect);
}

export function assertElasticSeatAllowed(
  sessionName: string,
  runtime: string | null | undefined,
  stateRoot?: string,
  continuationActionId?: string,
): void {
  if (continuationActionId) {
    const db = new Database(process.env.OPENRIG_DB ?? '', {readonly:true,fileMustExist:true});
    try { const {action}=assertElasticContinuationAllowed(continuationActionId,db);
      if(action.target.session!==sessionName || action.target.stateRoot!==stateRoot || action.effect.kind!=='resume-conversation') throw Error('elastic_continuation_denied');
    } finally {db.close();}
  } else assertElasticEffectAllowed();
  if (process.env.OPENRIG_ELASTIC_GUARD === undefined) return;
  try {
    const path = process.env.OPENRIG_ELASTIC_MANIFEST!;
    const st = lstatSync(path);
    if (
      !st.isFile() ||
      st.uid !== 0 ||
      st.mode & 0o022 ||
      realpathSync(path) !== path ||
      st.size > 65536
    )
      throw Error();
    const bytes = readFileSync(path);
    if (
      createHash("sha256").update(bytes).digest("hex") !==
      process.env.OPENRIG_ELASTIC_MANIFEST_DIGEST
    )
      throw Error();
    const m = JSON.parse(bytes.toString());
    if (
      runtime !== "pi" ||
      !m.linux.seats.includes(sessionName) ||
      (stateRoot !== undefined && stateRoot !== m.linux.stateRoot)
    )
      throw Error();
  } catch {
    throw Error("elastic_worker_profile_denied");
  }
}

/** Repeated at the actual spawn/transport boundary after async adapter work. */
export function assertElasticTmuxEffectAllowed(argv: readonly string[]): void {
  if(argv[0]!=='tmux')argv=['tmux',...argv];
  const scope=transportScope.getStore();
  if(scope) {
    const db=new Database(process.env.OPENRIG_DB??'',{readonly:true,fileMustExist:true});
    try {
      const {action,stage}=assertElasticContinuationAllowed(scope.actionId,db);
      if(workerDigest(argv)!==scope.argvDigest || argv[0]!=='tmux' || argv[1]!=='send-keys' || argv[2]!=='-t'
        || argv[3]!==action.target.pane || stage!=='transport-issued') throw Error('elastic_continuation_denied');
      const manifest=continuationManifest();
      const type=['tmux','send-keys','-t',action.target.pane,'-l','--',continuationTransportPlan(action,manifest)];
      const enter=['tmux','send-keys','-t',action.target.pane,'Enter'];
      if(workerDigest(argv)!==workerDigest(type)&&workerDigest(argv)!==workerDigest(enter))throw Error('elastic_continuation_denied');
      verifyContinuationProcess(action.target,action.effect.kind==='existing-runner'?'live':'absent',manifest);
      return;
    } finally {db.close();}
  }
  if (["new-session", "new-window", "split-window", "respawn-pane", "send-keys", "paste-buffer", "load-buffer", "source-file", "run-shell"].some(command => argv.includes(command))) assertElasticEffectAllowed();
}
