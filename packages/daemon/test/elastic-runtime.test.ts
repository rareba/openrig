import { afterEach, expect, it, vi } from 'vitest';
import { rmSync, chmodSync, writeFileSync, realpathSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { SessionRegistry } from '../src/domain/session-registry.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { EventBus } from '../src/domain/event-bus.js';
import { accountWorkerCapacity, type CapacitySeat } from '../src/adapters/elastic-worker-capacity.js';
import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { createHash } from 'node:crypto';
import { loadElasticDeploymentProfile } from '../src/adapters/elastic-deployment-profile.js';
import { bindCoordinatorReadinessPlan } from '../src/adapters/elastic-coordinator-admission.js';
import { enrollmentDigest } from '../src/adapters/elastic-worker-attempts.js';
import { managedHostId } from '../src/adapters/elastic-worker-enrollment.js';
import { ElasticWorkerControl } from '../src/domain/elastic-worker-control.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { elasticWorkerRoutes } from '../src/routes/elastic-worker.js';
import { humanResponsesRoutes } from '../src/routes/human-responses.js';
import { queueRoutes } from '../src/routes/queue.js';
import { createElasticRuntime } from '../src/adapters/elastic-runtime.js';
import { elasticProfileFixture } from './helpers/elastic-profile.js';
const cleanup: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });
function fixture(manager = true) {
  const profile = elasticProfileFixture(); cleanup.push(() => rmSync(profile.root, { recursive: true, force: true }));
  const databasePath = join(profile.root, 'native.sqlite'), db = createDb(databasePath); migrate(db, ALL_MIGRATIONS); cleanup.push(() => db.close());
  const sessions = new SessionRegistry(db);
  if (manager) {
    db.prepare("INSERT INTO rigs(id,name) VALUES('fixture','fixture')").run();
    db.prepare("INSERT INTO nodes(id,rig_id,logical_id,role,runtime) VALUES('manager','fixture','manager','manager','pi')").run();
    sessions.registerSession('manager', 'manager@fixture');
  }
  const options = { db, databasePath, sessions, hostId: 'coordinator', profilePath: profile.profilePath };
  return { ...profile, db, sessions, options };
}
it('absent profile has zero file/owner/runtime effects', () => {
  const db = createDb(); migrate(db, ALL_MIGRATIONS); cleanup.push(() => db.close());
  const runtime = createElasticRuntime({ db, databasePath: '/not-read', sessions: new SessionRegistry(db), hostId: 'coordinator' });
  expect(runtime).toBeUndefined(); expect(db.prepare('SELECT * FROM elastic_native_owners').all()).toEqual([]);
});
it('constructs same-queue native authority with real manager and keeps disabled/dry-run/status/plan inert', async () => {
  const f = fixture();
  // No executable/artifact exists and the private bind address is unavailable. Inert composition must still work.
  const runtime = createElasticRuntime(f.options)!; cleanup.push(() => runtime.close());
  const queue = new QueueRepository(f.db, new EventBus(f.db), { validateRig: () => true, elasticHumanProxyAuthority: runtime.directory.authority() });
  const service = runtime.attach(queue, { listIssues: async () => [], observeTargets: async () => [], getTask: async () => null, createTask: async (_host, input) => queue.create({ ...input, nudge: false }) });
  expect(service.queue).toBe(queue);
  expect(runtime.directory.authority().isLocalManager('manager@fixture')).toBe(true);
  expect(runtime.directory.authority().currentWorker('unknown')).toBeNull();
  const provider = { kind: 'azure-vmss', subscription: '00000000-0000-0000-0000-000000000001', resourceGroup: 'fixture', scaleSetName: 'fixture', resourceId: f.manifest.resourceId,
    poolId: 'fixture', ownershipTag: { key: 'pool', value: 'fixture' }, image: { kind: 'marketplace', publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' },
    bootstrap: { path: '/etc/openrig-elastic/manifest.json', digest: JSON.parse(requirePlan(f)).manifestDigest }, deploymentProfileRef: f.profilePath };
  const policy = { poolId: 'fixture', enabled: false, mode: 'dry-run' as const, maxWorkers: 1, slotsPerWorker: 2, capabilities: ['github:1:public-read'], pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900 };
  runtime.service.configure({ policy, provider });
  await runtime.service.plan(); await runtime.service.reconcile();
  runtime.service.configure({ policy: { ...policy, enabled: true }, provider });
  await runtime.service.plan(); await runtime.service.reconcile();
  runtime.service.configure({ policy: { ...policy, mode: 'active' }, provider });
  await runtime.service.reconcile();
  runtime.service.configure({ policy: { ...policy, enabled: true, mode: 'active' }, provider });
  await runtime.owner.run('fleet', () => runtime.service.plan());
  expect(runtime.store.list()).toEqual([]);
  expect(f.db.prepare('SELECT * FROM elastic_fleet_operations').all()).toEqual([]);
  expect(f.db.prepare('SELECT * FROM elastic_native_owners').all()).toEqual([]);
});
import { readFileSync } from 'node:fs';
function requirePlan(f: ReturnType<typeof fixture>) { return readFileSync(f.profile.preparationPlanPath, 'utf8'); }
it('rejects missing manager, nonprivate profile and unknown profile fields before external setup', () => {
  const missing = fixture(false); expect(() => createElasticRuntime(missing.options)).toThrow('native_readiness_owner_required');
  const privateFile = fixture(); chmodSync(privateFile.profilePath, 0o644); expect(() => createElasticRuntime(privateFile.options)).toThrow('elastic_private_file_rejected');
  const unknown = fixture(); writeFileSync(unknown.profilePath, JSON.stringify({ ...unknown.profile, telemetry: { ready: true } })); expect(() => createElasticRuntime(unknown.options)).toThrow();
});

it('rejects changed pinned Azure entry bytes before active setup or ARM invocation', async () => {
  const f = fixture();
  const executable = join(f.root, 'az'); writeFileSync(executable, '#!/bin/sh\nexit 97\n', { mode: 0o700 });
  f.put('profile.json', { ...f.profile, azure: { ...f.profile.azure, executable } });
  const runtime = createElasticRuntime(f.options)!; cleanup.push(() => runtime.close());
  const queue = new QueueRepository(f.db, new EventBus(f.db));
  runtime.attach(queue, { listIssues: async () => [], observeTargets: async () => [], getTask: async () => null, createTask: async (_host, input) => queue.create({ ...input, nudge: false }) });
  runtime.service.configure({ policy: { poolId: 'fixture', enabled: true, mode: 'active', maxWorkers: 1, slotsPerWorker: 2, capabilities: ['github:1:public-read'], pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900 },
    provider: { kind: 'azure-vmss', subscription: '00000000-0000-0000-0000-000000000001', resourceGroup: 'fixture', scaleSetName: 'fixture', resourceId: f.manifest.resourceId,
      poolId: 'fixture', ownershipTag: { key: 'pool', value: 'fixture' }, image: { kind: 'marketplace', publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' },
      bootstrap: { path: '/etc/openrig-elastic/manifest.json', digest: JSON.parse(requirePlan(f)).manifestDigest }, deploymentProfileRef: f.profilePath } });
  expect((await runtime.service.reconcile()).error).toBe('observation_failed');
  expect(runtime.store.list()).toEqual([]); expect(f.db.prepare('SELECT * FROM elastic_fleet_operations').all()).toEqual([]);
});

it('uses the real directory and SAME queue/owner to forward a native answer on its independent timer while fleet is paused', async () => {
  const f = fixture();
  const vmId = '11111111-1111-4111-8111-111111111111', childId = f.manifest.resourceId + '/virtualMachines/0';
  const imageReference = { publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' };
  // A real pinned local executable supplies deterministic ARM JSON; no Azure account or network is used.
  const executable = join(f.root, 'fixture-az');
  const script = `#!${realpathSync(process.execPath)}\nconst a=process.argv.slice(2); const child=${JSON.stringify({ id: childId, vmId, instanceId: '0', protectionPolicy: { protectFromScaleIn: true }, storageProfile: { imageReference }, networkProfile: { networkInterfaces: [{ id: childId + '/networkInterfaces/nic' }] } })};
  let value; if(a[1]==='nic') value=[{id:child.id+'/networkInterfaces/nic',virtualMachine:{id:child.id},ipConfigurations:[{privateIPAddress:'10.0.0.2'}]}];
  else if(a.includes('--instance-id')) value=child;
  else if(a[0]==='vmss' && a[1]==='show') value=${JSON.stringify({ id: f.manifest.resourceId, location: 'fixture', tags: { pool: 'fixture' }, virtualMachineProfile: { storageProfile: { imageReference } } })};
  else throw Error('unexpected external effect'); process.stdout.write(JSON.stringify(value));`;
  writeFileSync(executable, script, { mode: 0o700 });
  f.put('profile.json', { ...f.profile, azure: { ...f.profile.azure, executable, sha256: createHash('sha256').update(readFileSync(executable)).digest('hex') } });
  const loaded = loadElasticDeploymentProfile(f.profilePath);
  const runtime = createElasticRuntime(f.options)!;
  const queue = new QueueRepository(f.db, new EventBus(f.db), { validateRig: () => true, elasticHumanProxyAuthority: runtime.directory.authority() });
  queue.attachOutbox(new OutboxHandler(f.db));
  const issue = { repositoryId: '1', repository: 'fixture/repo', number: 1, url: 'https://github.com/fixture/repo/issues/1', title: 'Fixture', body: 'Synthetic assigned work', updatedAt: 'revision-1', state: 'open' as const, assignees: ['operator'] };
  const intake = runtime.attach(queue, { listIssues: async () => [issue], observeTargets: async () => [], getTask: async () => null, createTask: async (_host, input) => queue.create({ ...input, nudge: false }) });
  const provider = { kind: 'azure-vmss', subscription: '00000000-0000-0000-0000-000000000001', resourceGroup: 'fixture', scaleSetName: 'fixture', resourceId: f.manifest.resourceId,
    poolId: 'fixture', ownershipTag: { key: 'pool', value: 'fixture' }, image: { kind: 'marketplace', ...imageReference },
    bootstrap: { path: '/etc/openrig-elastic/manifest.json', digest: loaded.preparationPlan.manifestDigest }, deploymentProfileRef: f.profilePath };
  runtime.service.configure({ policy: { poolId: 'fixture', enabled: true, mode: 'active', maxWorkers: 1, slotsPerWorker: 2, capabilities: ['github:1:public-read'], pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900 }, provider });
  const plan = bindCoordinatorReadinessPlan({ plan: loaded.preparationPlan, sessions: f.sessions, manager: f.profile.coordinatorManager, hostId: f.options.hostId });
  const configurationDigest = enrollmentDigest({ manifest: plan.manifestDigest, plan, artifact: loaded.artifact, recipe: loaded.packageRecipe, stage: f.profile.stageScript, helper: f.profile.helperScript,
    checkpointCaPem: loaded.caPem, epoch: f.profile.fleetEpoch, commit: f.profile.workerBuildCommit });
  const attempt = await runtime.owner.run('fleet', async () => {
    const a = runtime.store.createOrRead({ resourceId: f.manifest.resourceId, childResourceId: childId, instanceId: '0', vmId, operationId: 'provision-1', fleetEpoch: f.profile.fleetEpoch,
      manifestDigest: plan.manifestDigest, artifactDigest: loaded.artifact.archiveSha256, specDigest: plan.specDigest, checkoutDigest: enrollmentDigest(plan.seats), configurationDigest });
    runtime.store.saveBearer(a.id, 'fixture-bearer-'.repeat(4));
    return runtime.store.advance(a.id, a.revision, { phase: 'ready', registration: { hostId: managedHostId(a.workerId), selfHostId: 'worker-host', baseUrl: `http://10.0.0.2:${f.manifest.port}` } });
  });
  const endpoint = { instance: { version: 1, imageReference: f.manifest.linux.imageReference, resourceId: f.manifest.resourceId, vmId, manifestDigest: plan.manifestDigest, fleetEpoch: f.profile.fleetEpoch, operationId: 'provision-1', privateIPv4: '10.0.0.2' }, baseUrl: attempt.registration!.baseUrl, location: 'fixture' };
  runtime.store.savePrivateInput(attempt.id, 'trusted-endpoint', endpoint);
  f.put('hosts.yaml', { hosts: [{ id: attempt.registration!.hostId, transport: 'http', hostId: 'worker-host', url: endpoint.baseUrl, bearer_file: runtime.store.tokenPath(attempt.id) }] });
  const originDb = createDb(); migrate(originDb, ALL_MIGRATIONS); cleanup.push(() => originDb.close());
  const control = new ElasticWorkerControl(originDb); control.enroll({ workerId: attempt.workerId, vmId, manifestDigest: plan.manifestDigest });
  const origin = new QueueRepository(originDb, new EventBus(originDb), { validateRig: () => true }); origin.attachOutbox(new OutboxHandler(originDb));
  await origin.create({ qitemId: 'question-from-managed-seat', sourceSession: 'worker-a@elastic', destinationSession: 'human@kernel', body: 'Proceed?', summary: 'Confirm the synthetic operation', evidenceRef: 'fixture:evidence', humanQuestions: [{ id: 'go', question: 'Proceed?', options: [{ id: 'yes', label: 'Proceed' }, { id: 'no', label: 'Wait' }] }], nudge: false });
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('queueRepo' as never, origin as never); await next(); });
  app.get('/api/elastic-worker/probe', c => {
    const now = Date.now();
    const proc = (pid: number, ppid: number, argv: string[]) => ({ pid, ppid, startTime: String(pid), uid: f.manifest.uid, cgroup: '/system.slice/openrig-elastic.service', executable: f.manifest.linux.node, argv, state: 'S' });
    const seats: CapacitySeat[] = plan.seats.map((s, i) => ({ sessionName: s.sessionName, nodeId: `seat-${i}`, generation: `generation-${i}`, cwd: `/fixture/${i}`, runtime: 'pi', model: plan.model, launchId: `launch-${i}`,
      runner: proc(20 + i * 10, 10, [f.manifest.linux.node, resolve(f.manifest.linux.helper, '../pi-runner.js'), '--launch-id', `launch-${i}`]), child: proc(21 + i * 10, 20 + i * 10, [f.manifest.linux.node, f.manifest.linux.pi]), descendants: [], occupancy: i === 0 ? 'occupied' : 'free' }));
    const inventory = [proc(10, 1, [f.manifest.linux.node, f.manifest.linux.daemon]), ...seats.flatMap(s => [s.runner, s.child])];
    const capacity = accountWorkerCapacity(loaded.manifest, seats, inventory, 10, now);
    return c.json({ version: 1, observedAt: now, selfHostId: 'worker-host', build: { commit: f.profile.workerBuildCommit, dirty: false }, state: control.read(), ledger: control.ledger(),
      filesystem: { complete: true, blockers: [], files: [], refs: [], digest: 'f'.repeat(64) }, process: { observedAt: now, complete: true, idle: true, processes: 1, uid: f.manifest.uid, cgroup: '/system.slice/openrig-elastic.service' }, checkpointAcknowledged: false, drainEligible: false, clean: false,
      preparation: { version: 1, observedAt: now, manifestDigest: plan.manifestDigest, operationId: plan.operationId, planDigest: enrollmentDigest(plan), state: 'ready', rung: 'tool-proven', rigId: 'elastic',
        seats: plan.seats.map((s, i) => ({ sessionName: s.sessionName, repositoryId: s.repository.id, nodeId: `seat-${i}`, generation: `generation-${i}`, cwd: `/fixture/${i}`, runtime: 'pi', model: plan.model, launched: true, providerReady: true, toolProven: true, active: i === 0 })), repositoryCapabilities: [{ repositoryId: 1, publicRead: true, privateRead: false, write: false }], capacity, publicReason: 'fixture' } });
  });
  app.route('/api/elastic-worker', elasticWorkerRoutes({ bearerToken: 'fixture-bearer-'.repeat(4), queue: origin }));
  app.route('/api/mission-control', humanResponsesRoutes({ queueRepo: origin, bearerToken: 'fixture-bearer-'.repeat(4) })); app.route('/api/queue', queueRoutes());
  const server = serve({ fetch: app.fetch, port: 0, hostname: '127.0.0.1' }); await new Promise<void>(r => server.once('listening', r));
  cleanup.push(() => new Promise<void>((resolve, reject) => server.close(e => e ? reject(e) : resolve())));
  const address = server.address(); if (!address || typeof address === 'string') throw Error();
  const originalFetch = globalThis.fetch;
  vi.stubGlobal('fetch', (input: string | URL | Request, init?: RequestInit) => {
    const url = new URL(String(input)); if (url.origin !== endpoint.baseUrl) throw Error('unexpected network');
    url.hostname = '127.0.0.1'; url.port = String(address.port); return originalFetch(url, init);
  });
  cleanup.push(() => vi.unstubAllGlobals()); cleanup.push(() => runtime.close());
  intake.configure({ enabled: true, assignee: 'operator', repositories: [{ id: '1', name: 'fixture/repo' }], targets: [], hostCapacity: 2 });
  expect((await intake.reconcile()).error).toBeNull();
  intake.markEligibility({ repositoryId: '1', number: 1, revision: issue.updatedAt, eligibility: 'ready', capability: 'github:1:public-read', reason: 'Synthetic reviewed source' });
  expect((await intake.reconcile()).error).toBeNull();
  const assigned = intake.status().issues[0]!;
  expect(assigned.session).toBe('worker-b@elastic');
  expect(origin.getById(assigned.qitemId!)?.destinationSession).toBe('worker-b@elastic');
  expect(intake.queue).toBe(queue); expect(intake.status().config?.targets).toEqual([]);
  const originWakeBaseline = (originDb.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get() as { n: number }).n;
  runtime.service.pause();
  // This same owner is occupied by the fleet throughout; the independent scope must still deliver.
  let release!: () => void;
  const held = runtime.owner.run('fleet', () => new Promise<void>(resolve => { release = resolve; }));
  cleanup.push(async () => { release(); await held; });
  runtime.start();
  await vi.waitFor(() => expect(queue.listAttention()).toHaveLength(1), { timeout: 10000 });
  const proxy = queue.getById(queue.listAttention()[0]!.qitemId)!;
  expect(runtime.directory.authority().currentWorker(attempt.workerId)?.vmId).toBe(vmId);
  queue.recordHumanResponse({ qitemId: proxy.qitemId, replyId: 'runtime-answer', requestRevision: proxy.humanRequestRevision!, actorSession: 'human@kernel', response: { kind: 'choice', questionId: 'go', optionId: 'yes' } });
  await vi.waitFor(() => expect(origin.getById('question-from-managed-seat')?.state).toBe('done'), { timeout: 15000 });
  expect(f.db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: 0 });
  expect(originDb.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get()).toEqual({ n: originWakeBaseline + 1 });
  expect(f.db.prepare('SELECT * FROM elastic_fleet_operations').all()).toEqual([]);
}, 30000);
