import { afterEach, expect, it } from 'vitest';
import { spawn } from 'node:child_process';
import { mkdtempSync, realpathSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { ElasticNativeOwner, nativeOwnerIdentity, nativeOwnerStopped } from '../src/adapters/elastic-native-owner.js';
import { withHostRegistryLock } from '../src/domain/hosts/hosts-registry-lock.js';
import { addHostEntry } from '../src/domain/hosts/hosts-registry-writer.js';
import { loadHostRegistry } from '../src/domain/hosts/hosts-registry-reader.js';
const cleanup: Array<() => void> = [];
afterEach(() => cleanup.splice(0).reverse().forEach(fn => fn()));
function root() { const dir = realpathSync(mkdtempSync(join(tmpdir(), 'elastic-owner-'))); cleanup.push(() => rmSync(dir, { recursive: true, force: true })); return dir; }
async function deadOwner(file: string, scope: string, claimFleet = false) {
    const child = spawn(process.execPath, ['--input-type=module', '-e', `import Database from 'better-sqlite3'; import {ElasticNativeOwner} from ${JSON.stringify(new URL('../src/adapters/elastic-native-owner.ts', import.meta.url).href)}; const db=new Database(process.argv[1]); const owner=new ElasticNativeOwner(db,process.argv[1]); await owner.run(process.argv[2],async()=>{${claimFleet ? `owner.claimFleet(process.pid+':'+crypto.randomUUID());` : ''}process.stdout.write(JSON.stringify(owner.identity)+'\\n');await new Promise(()=>{});});`, file, scope], { env: { ...process.env, NODE_OPTIONS: '--import tsx' }, stdio: ['ignore', 'pipe', 'pipe'] });
    let text = '';
    await new Promise<void>((resolve, reject) => { child.stdout.on('data', chunk => { text += chunk; if (text.includes('\n'))
        resolve(); }); child.once('exit', code => { if (!text)
        reject(Error('fixture child exit ' + code)); }); });
    const identity = JSON.parse(text.trim());
    const exited = once(child, 'exit');
    child.kill('SIGKILL');
    await exited;
    return identity;
}
it('recovers exact stopped native SQLite owner while live/other-boot/legacy/DB drift remain blocked', async () => {
    const dir = root(), path = join(dir, 'db.sqlite');
    const db = createDb(path);
    cleanup.push(() => db.close());
    migrate(db, ALL_MIGRATIONS);
    const first = new ElasticNativeOwner(db, path), second = new ElasticNativeOwner(db, path);
    await first.run('consumer', async () => { await expect(second.run('consumer', async () => { })).rejects.toThrow('live_or_unknown'); });
    const prior = await deadOwner(path, 'fleet', true);
    expect(nativeOwnerStopped(prior, first.identity)).toBe(true);
    const saved = db.prepare("SELECT owner FROM elastic_native_owners WHERE scope='fleet-reconcile'").get() as {owner:string};
    expect(db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get()).toEqual({reconcile_owner:saved.owner});
    await deadOwner(path, 'fleet'); // A second crash after native guard recovery must not replace the original association.
    expect(db.prepare("SELECT owner FROM elastic_native_owners WHERE scope='fleet-reconcile'").get()).toEqual(saved);
    await first.run('fleet', async () => { first.recoverFleet(); expect(db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get()).toEqual({ reconcile_owner: null }); });
    for (const changed of [{ ...prior, boot: 'another-boot' }, { ...prior, host: 'another-host' }, { ...prior, database: 'another-db' }, {}]) {
        db.prepare("INSERT OR REPLACE INTO elastic_native_owners VALUES('bad','saved',?)").run(JSON.stringify(changed));
        await expect(first.run('bad', async () => { })).rejects.toThrow();
        expect(db.prepare("SELECT owner FROM elastic_native_owners WHERE scope='bad'").get()).toEqual({ owner: 'saved' });
    }
});
it('serializes real concurrent native host additions without lost manual entries and retains partial locks', async () => {
    const dir = root(), path = join(dir, 'hosts.yaml');
    const module = new URL('../src/domain/hosts/hosts-registry-writer.ts', import.meta.url).href;
    const children = Array.from({ length: 4 }, (_, i) => spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', `import {addHostEntry} from ${JSON.stringify(module)}; const r=addHostEntry({id:process.argv[2],transport:'http',url:'http://127.0.0.1:7433'},process.argv[1]); if(!r.ok){process.stderr.write(r.error);process.exitCode=1}`, path, 'host-' + i], { stdio: 'pipe' }));
    const statuses = await Promise.all(children.map(c => once(c, 'exit')));
    expect(statuses.map(s => s[0])).toEqual([0, 0, 0, 0]);
    const loaded = loadHostRegistry(path);
    expect(loaded.ok && loaded.registry.hosts.map(h => h.id).sort()).toEqual(['host-0', 'host-1', 'host-2', 'host-3']);
    const before = readFileSync(path, 'utf8');
    writeFileSync(path + '.writer-lock', '', { mode: 0o600 });
    expect(addHostEntry({ id: 'new-host', transport: 'http', url: 'http://127.0.0.1:7433' }, path)).toMatchObject({ ok: false, error: expect.stringContaining('busy_or_unknown') });
    expect(readFileSync(path, 'utf8')).toBe(before);
});
it('detects direct external whole-file drift and never overwrites it as a false CAS', () => {
    const dir = root(), path = join(dir, 'hosts.yaml');
    writeFileSync(path, 'original');
    expect(() => withHostRegistryLock(path, unchanged => { writeFileSync(path, 'manual external edit'); unchanged(); writeFileSync(path, 'stale'); })).toThrow('external_drift');
    expect(readFileSync(path, 'utf8')).toBe('manual external edit');
});
it('recovers a registry lock only after its actual native subprocess has stopped', async () => {
    const dir = root(), path = join(dir, 'hosts.yaml');
    const module = new URL('../src/domain/hosts/hosts-registry-lock.ts', import.meta.url).href;
    const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', `import {withHostRegistryLock} from ${JSON.stringify(module)};withHostRegistryLock(process.argv[1],()=>{process.stdout.write('locked\\n');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,100000);});`, path], { stdio: ['ignore', 'pipe', 'pipe'] });
    await once(child.stdout, 'data');
    const lock = readFileSync(path + '.writer-lock', 'utf8');
    expect(JSON.parse(lock).identity.pid).toBe(child.pid);
    const exited = once(child, 'exit');
    child.kill('SIGKILL');
    await exited;
    expect(addHostEntry({ id: 'recovered', transport: 'http', url: 'http://127.0.0.1:7433' }, path)).toMatchObject({ ok: true });
    expect(loadHostRegistry(path)).toMatchObject({ ok: true, registry: { hosts: [{ id: 'recovered' }] } });
});
import { ElasticWorkerAttemptStore } from '../src/adapters/elastic-worker-attempts.js';
import { chmodSync, mkdirSync, existsSync } from 'node:fs';
it('recovers guarded attempt locks from exact stopped native owner without replacing its pending command', async () => {
    const dir = root(), path = join(dir, 'db.sqlite');
    chmodSync(dir, 0o700);
    const db = createDb(path);
    cleanup.push(() => db.close());
    migrate(db, ALL_MIGRATIONS);
    const owner = new ElasticNativeOwner(db, path);
    const privateRoot = join(dir, 'attempts');
    mkdirSync(privateRoot, { mode: 0o700 });
    const store = new ElasticWorkerAttemptStore(privateRoot, owner);
    let id = '';
    await owner.run('fleet', async () => { const attempt = store.createOrRead({ resourceId: '/resource', childResourceId: '/resource/virtualMachines/0', instanceId: '0', vmId: '11111111-1111-4111-8111-111111111111', operationId: 'operation', fleetEpoch: 'epoch', manifestDigest: 'a'.repeat(64), artifactDigest: 'b'.repeat(64), specDigest: 'c'.repeat(64), checkoutDigest: 'd'.repeat(64), configurationDigest: 'e'.repeat(64) }); id = attempt.id; store.advance(id, 0, { command: { name: 'original-command', operation: 'stage', requestDigest: 'f'.repeat(64), state: 'unknown' } }); });
    const lock = join(privateRoot, id, 'advance.lock');
    const module = new URL('../src/adapters/elastic-native-owner.ts', import.meta.url).href;
    const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', `import Database from 'better-sqlite3';import {openSync,writeFileSync,fsyncSync,closeSync} from 'node:fs';import {ElasticNativeOwner,nativeOwnerIdentity} from ${JSON.stringify(module)};const db=new Database(process.argv[1]);await new ElasticNativeOwner(db,process.argv[1]).run('fleet',async()=>{const fd=openSync(process.argv[2],'wx',0o600);writeFileSync(fd,JSON.stringify(nativeOwnerIdentity(process.argv[2])));fsyncSync(fd);closeSync(fd);process.stdout.write('locked\\n');await new Promise(()=>{});});`, path, lock], { stdio: ['ignore', 'pipe', 'pipe'] });
    await once(child.stdout, 'data');
    const exited = once(child, 'exit');
    child.kill('SIGKILL');
    await exited;
    const before = store.read(id);
    await owner.run('fleet', async () => { store.recoverStoppedLocks(); expect(existsSync(lock)).toBe(false); expect(store.read(id)).toEqual(before); expect(() => store.advance(id, before.revision, { command: { ...before.command!, name: 'replacement' } })).toThrow(); store.advance(id, before.revision, { command: { ...before.command!, state: 'submitted' } }); });
    expect(store.read(id).command?.name).toBe('original-command');
});
it('retains a same-PID unrelated fleet reconcile UUID without an exact native acquisition association', async () => {
    const dir = root(), path = join(dir, 'db.sqlite'); const db = createDb(path); cleanup.push(() => db.close()); migrate(db, ALL_MIGRATIONS);
    const owner = new ElasticNativeOwner(db, path); const prior = await deadOwner(path, 'fleet', true);
    const association = db.prepare("SELECT owner,identity FROM elastic_native_owners WHERE scope='fleet-reconcile'").get();
    const unrelated = `${prior.pid}:00000000-0000-4000-8000-000000000001`;
    db.prepare('UPDATE elastic_fleet_config SET reconcile_owner=? WHERE id=1').run(unrelated);
    await owner.run('fleet', async () => { expect(() => owner.recoverFleet()).toThrow(); });
    expect(db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get()).toEqual({ reconcile_owner: unrelated });
    expect(db.prepare("SELECT owner,identity FROM elastic_native_owners WHERE scope='fleet-reconcile'").get()).toEqual(association);
    db.prepare("DELETE FROM elastic_native_owners WHERE scope='fleet-reconcile'").run();
    await owner.run('fleet', async () => { expect(() => owner.recoverFleet()).toThrow('unproven'); });
    expect(db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get()).toEqual({ reconcile_owner: unrelated });
});

import { ElasticFleetService, type FleetIO } from '../src/domain/elastic-fleet.js';
it('recovers the exact recorded fleet claim then inspects the original pending operation without another mutation', async () => {
    const dir=root(),path=join(dir,'db.sqlite');const db=createDb(path);cleanup.push(()=>db.close());migrate(db,ALL_MIGRATIONS);
    const ownerModule=new URL('../src/adapters/elastic-native-owner.ts',import.meta.url).href;
    const fleetModule=new URL('../src/domain/elastic-fleet.ts',import.meta.url).href;
    const program=`import Database from 'better-sqlite3';import {ElasticNativeOwner} from ${JSON.stringify(ownerModule)};import {ElasticFleetService} from ${JSON.stringify(fleetModule)};
const db=new Database(process.argv[1]);const owner=new ElasticNativeOwner(db,process.argv[1]);let time=10000;
const io={observe:async()=>({now:time,observedAt:time,providerAvailable:true,workers:[],demands:[{id:'original-issue',capability:'code'}]}),provision:async operation=>{process.stdout.write(JSON.stringify(operation)+'\\n');await new Promise(()=>{});},beginDrain:async()=>{throw Error('unexpected drain')},remove:async()=>{throw Error('unexpected remove')},inspectOperation:async()=>{throw Error('unexpected inspection')}};
const service=new ElasticFleetService(db,io,()=>time,owner);service.configure({policy:{poolId:'fixture',enabled:true,mode:'active',maxWorkers:1,slotsPerWorker:2,capabilities:['code'],pollIntervalSeconds:10,scaleOutSamples:2,idleSeconds:900}});await service.reconcile();time+=10000;await service.reconcile();`;
    const child=spawn(process.execPath,['--import','tsx','--input-type=module','-e',program,path],{stdio:['ignore','pipe','pipe']});
    let bytes='';await new Promise<void>((resolve,reject)=>{child.stdout.on('data',chunk=>{bytes+=chunk;if(bytes.includes('\n'))resolve();});child.once('exit',code=>{if(!bytes)reject(Error('fleet fixture exited '+code));});});
    const submitted=JSON.parse(bytes.trim());const saved=db.prepare("SELECT owner,identity FROM elastic_native_owners WHERE scope='fleet-reconcile'").get() as {owner:string;identity:string};
    expect(db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get()).toEqual({reconcile_owner:saved.owner});expect(saved.owner).toMatch(new RegExp('^'+child.pid+':'));
    const exited=once(child,'exit');child.kill('SIGKILL');await exited;
    const inspected:string[]=[];let mutations=0;let recovered=0;
    const owner=new ElasticNativeOwner(db,path);
    const io:FleetIO={recoverAttempts:()=>{owner.assertHeld('fleet');expect(db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get()).toEqual({reconcile_owner:null});recovered++;},observe:async()=>{throw Error('pending operation must be inspected first')},provision:async()=>{mutations++},beginDrain:async()=>{mutations++},remove:async()=>{mutations++},inspectOperation:async operation=>{inspected.push(operation.operationId);expect(operation.targetCapacity).toBe(submitted.targetCapacity);return 'pending';}};
    const restarted=new ElasticFleetService(db,io,()=>30000,owner);await restarted.reconcile();expect(recovered).toBe(1);expect(()=>owner.assertHeld('fleet')).toThrow('native_owner_required');
    expect(inspected).toEqual([submitted.operationId]);expect(mutations).toBe(0);expect(restarted.status().operation?.operationId).toBe(submitted.operationId);expect(restarted.status().operation?.state).toBe('pending');
    expect(db.prepare("SELECT owner FROM elastic_native_owners WHERE scope='fleet-reconcile'").get()).toBeUndefined();
});
