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
async function deadOwner(file: string, scope: string) {
    const child = spawn(process.execPath, ['--input-type=module', '-e', `import Database from 'better-sqlite3'; import {ElasticNativeOwner} from ${JSON.stringify(new URL('../src/adapters/elastic-native-owner.ts', import.meta.url).href)}; const db=new Database(process.argv[1]); const owner=new ElasticNativeOwner(db,process.argv[1]); await owner.run(process.argv[2],async()=>{process.stdout.write(JSON.stringify(owner.identity)+'\\n');await new Promise(()=>{});});`, file, scope], { env: { ...process.env, NODE_OPTIONS: '--import tsx' }, stdio: ['ignore', 'pipe', 'pipe'] });
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
    const prior = await deadOwner(path, 'fleet');
    expect(nativeOwnerStopped(prior, first.identity)).toBe(true);
    db.prepare("UPDATE elastic_fleet_config SET reconcile_owner=? WHERE id=1").run(`${prior.pid}:durable-original`);
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
