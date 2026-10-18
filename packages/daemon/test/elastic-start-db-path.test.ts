import {afterEach,describe,expect,it,vi} from 'vitest';
import {mkdtempSync,realpathSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import Database from 'better-sqlite3';
import {assertElasticUnitStartAllowed} from '../src/domain/elastic-worker-guard.js';
import {ElasticWorkerControl} from '../src/domain/elastic-worker-control.js';
import {migrate} from '../src/db/migrate.js';
import {ALL_MIGRATIONS} from '../src/db/all-migrations.js';
// Marker must be root-owned to pass the guard; fake uid 0 for the marker only.
vi.mock('node:fs',async original=>{const fs=await original<typeof import('node:fs')>();return {...fs,lstatSync:(path:any,...args:any[])=>{const stat=(fs.lstatSync as any)(path,...args);return String(path).endsWith('/root-owned-marker')?new Proxy(stat,{get:(target,key)=>key==='uid'?0:Reflect.get(target,key)}):stat;}};});
const dirs:string[]=[];
afterEach(()=>{delete process.env.OPENRIG_ELASTIC_GUARD;delete process.env.OPENRIG_DB;for(const d of dirs.splice(0))rmSync(d,{recursive:true,force:true});});
const identity={workerId:'worker',vmId:'11111111-1111-4111-8111-111111111111',manifestDigest:'a'.repeat(64)};
function setup(){
  const root=realpathSync(mkdtempSync(join(tmpdir(),'elastic-start-db-')));dirs.push(root);
  const marker=join(root,'root-owned-marker');
  writeFileSync(marker,JSON.stringify({version:1,phase:'ready',...identity}),{mode:0o644});
  const dbPath=join(root,'openrig.sqlite');
  const db=new Database(dbPath);migrate(db,ALL_MIGRATIONS);new ElasticWorkerControl(db).enroll(identity);db.close();
  process.env.OPENRIG_ELASTIC_GUARD=marker;
  return {root,marker,dbPath};
}
describe('unit-start guard checks the database the daemon will open',()=>{
  it('allows a valid resolved DB when OPENRIG_DB is unset (RIGGED_DB / OPENRIG_HOME default path)',()=>{
    const {dbPath}=setup();
    expect(()=>assertElasticUnitStartAllowed(undefined,dbPath)).not.toThrow();
  });
  it('prefers the resolved dbPath over a stale OPENRIG_DB',()=>{
    const {dbPath}=setup();
    process.env.OPENRIG_DB='/does/not/exist.sqlite';
    expect(()=>assertElasticUnitStartAllowed(undefined,dbPath)).not.toThrow();
  });
  it('denies when the resolved DB is missing (fail-closed, no raw SQLite error)',()=>{
    const {root}=setup();
    expect(()=>assertElasticUnitStartAllowed(undefined,join(root,'missing.sqlite'))).toThrow('elastic_worker_effect_denied');
  });
  it('denies when the resolved DB has no matching admission marker',()=>{
    const {marker,dbPath}=setup();
    writeFileSync(marker,JSON.stringify({version:1,phase:'ready',...identity,workerId:'other'}),{mode:0o644});
    expect(()=>assertElasticUnitStartAllowed(undefined,dbPath)).toThrow('elastic_worker_effect_denied');
  });
});
