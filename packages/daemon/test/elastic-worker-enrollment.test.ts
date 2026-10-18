import { accountWorkerCapacity, type CapacitySeat } from '../src/adapters/elastic-worker-capacity.js';
import { describe, expect, it } from 'vitest';
import { parseManagedWorkerProbe, managedHostId } from '../src/adapters/elastic-worker-enrollment.js';
const attempt = { workerId: 'fixture-worker', binding: { vmId: '11111111-1111-4111-8111-111111111111', manifestDigest: 'a'.repeat(64) } };
describe('managed native observation', () => {
    it('rejects health-only, missing native preparation and stale/future/changed identity', () => {
        expect(() => parseManagedWorkerProbe({ status: 'ok' }, attempt, 'b'.repeat(40), 10000)).toThrow();
        expect(managedHostId('fixture')).toMatch(/^elastic-[a-f0-9]{24}$/);
        const probe = { version: 1, observedAt: 10000, selfHostId: 'fixture-host', build: { commit: 'b'.repeat(40), dirty: false }, state: { version: 1, workerId: attempt.workerId, vmId: attempt.binding.vmId, manifestDigest: attempt.binding.manifestDigest, phase: 'ready', generation: 0, drainOperationId: null, drainStartedAt: null }, ledger: { complete: true, custody: 9, runnable: 2, claims: 1, unresolvedOutbox: 0, pendingHumanReturns: false, pendingActivationIntents: false, pendingTransfers: false, clean: false, digest: 'c'.repeat(64) }, filesystem: { complete: true, blockers: [], files: [], refs: [], digest: 'c'.repeat(64) }, process: { observedAt: 10000, complete: true, idle: false, processes: 3, uid: 490, cgroup: '/system.slice/openrig-elastic.service' }, checkpointAcknowledged: false, drainEligible: false, clean: false,
            preparation: { version: 1, observedAt: 10000, manifestDigest: attempt.binding.manifestDigest, operationId: 'fixture', planDigest: 'd'.repeat(64), state: 'pending', rung: 'enrolled', rigId: null, seats: [], repositoryCapabilities: [], publicReason: 'pending' } };
        expect(parseManagedWorkerProbe(probe, attempt, 'b'.repeat(40), 10000).ledger.custody).toBe(9);
        for (const changed of [{ observedAt: 10001 }, { observedAt: 1 }, { build: { commit: 'x', dirty: false } }, { preparation: undefined }])
            expect(() => parseManagedWorkerProbe({ ...probe, ...changed }, attempt, 'b'.repeat(40), 10000)).toThrow();
    });
});
import { afterEach } from 'vitest';
import { chmodSync, mkdtempSync, realpathSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createHash, createPublicKey, generateKeyPairSync } from 'node:crypto';
import { ElasticWorkerAttemptStore } from '../src/adapters/elastic-worker-attempts.js';
import { AzureWorkerCommandClient } from '../src/adapters/azure-worker-command.js';
import { ElasticWorkerEnrollment } from '../src/adapters/elastic-worker-enrollment.js';
import type { FleetOperation } from '../src/domain/elastic-fleet.js';
import { sealEnrollment } from '../src/domain/elastic-enrollment.js';
import { loadHostRegistry } from '../src/domain/hosts/hosts-registry-reader.js';
const roots: string[] = [];
afterEach(() => roots.splice(0).forEach(root => rmSync(root, { recursive: true, force: true })));
function fixture(unknown = false) {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'enrollment-default-')));
    roots.push(root);
    chmodSync(root, 0o700);
    const resourceId = '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture';
    const vmId = '11111111-1111-4111-8111-111111111111';
    const manifest = JSON.parse(readFileSync(resolve(import.meta.dirname, '../../../integrations/azure-elastic/manifest.example.json'), 'utf8'));
    manifest.resourceId = resourceId;
    manifest.linux.imageReference = 'Canonical:ubuntu:24:1.2.3';
    manifest.pins = manifest.pins.map((p: object) => ({ ...p, sha256: 'a'.repeat(64) }));
    for (const [key, file] of [['piAuthFile', 'auth'], ['piSettingsFile', 'settings'], ['piModelsFile', 'models']]) {
        const path = join(root, file!);
        writeFileSync(path, '{}', { mode: 0o600 });
        manifest.materialFiles[key!] = path;
    }
    const manifestRaw = JSON.stringify(manifest);
    const manifestDigest = createHash('sha256').update(manifestRaw).digest('hex');
    const configuration = { kind: 'azure-vmss' as const, subscription: '00000000-0000-0000-0000-000000000001', resourceGroup: 'fixture', scaleSetName: 'fixture', resourceId, poolId: 'fixture', ownershipTag: { key: 'pool', value: 'fixture' }, image: { kind: 'marketplace' as const, publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' }, bootstrap: { path: '/fixture/manifest.json', digest: manifestDigest } };
    const instance = { resourceId: `${resourceId}/virtualMachines/0`, instanceId: '0', vmId, workerId: `azure:${resourceId.toLowerCase()}:${vmId}`, protected: true, provisioningState: 'Succeeded' };
    const policy = { poolId: 'fixture', enabled: true, mode: 'active' as const, maxWorkers: 1, slotsPerWorker: 2, capabilities: ['github:1:public-read'], pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900 };
    const context = { configuration, instance, operation: { operationId: 'fixture-enrollment', type: 'provision' as const, state: 'pending' as const, workerId: null, count: 1, targetCapacity: 1, revision: 1, snapshot: { observedAt: 10000, now: 10000, demands: [], workers: [], providerAvailable: true }, error: null, createdAt: 10000, updatedAt: 10000, policy, configuration: { policy } } };
    const preparationPlan = { version: 1 as const, operationId: 'fixture-preparation', manifestDigest, specDigest: 'b'.repeat(64), model: 'fixture/model', approvedActionKeys: [], repositoryAccess: { mode: 'public-readonly' as const }, seats: ['worker-a@elastic', 'worker-b@elastic'].map(sessionName => ({ sessionName, repository: { id: 1, name: 'fixture/repo', baseRef: 'refs/heads/main', sha: 'a'.repeat(40) } })), owner: null };
    const scriptPath = join(root, 'script.py');
    writeFileSync(scriptPath, 'print("fixed")\n', { mode: 0o600 });
    const script = { path: scriptPath, sha256: createHash('sha256').update(readFileSync(scriptPath)).digest('hex') };
    const commands = new Map<string, {
        output: string;
        digest: string;
    }>();
    let puts = 0;
    let providerReady = false;
    let busy = false;
    const proc=(pid:number,ppid:number,argv:string[])=>({pid,ppid,startTime:String(pid),uid:490,cgroup:'/system.slice/openrig-elastic.service',executable:manifest.linux.node,argv,state:'S'});
    const capacity=()=>{const seats:CapacitySeat[]=preparationPlan.seats.map((s,i)=>({sessionName:s.sessionName,nodeId:s.sessionName,generation:'fixture-generation',cwd:'/fixture/work',runtime:'pi',model:preparationPlan.model,launchId:'launch-'+i,runner:proc(20+i*10,10,[manifest.linux.node,resolve(manifest.linux.helper,'../pi-runner.js'),'--launch-id','launch-'+i]),child:proc(21+i*10,20+i*10,[manifest.linux.node,manifest.linux.pi]),descendants:[],occupancy:busy&&i===0?'occupied':'free'})); const inventory=[proc(10,1,[manifest.linux.node,manifest.linux.daemon]),...seats.flatMap(s=>[s.runner,s.child]),...(busy?[{...proc(22,21,['/bin/bash','-c','ongoing']),executable:'/bin/bash'}]:[])]; return accountWorkerCapacity(manifest,seats,inventory,10,10000);};
    let controllerKey = '';
    const key = generateKeyPairSync('rsa', { modulusLength: 3072 });
    const store = new ElasticWorkerAttemptStore(root);
    const client = new AzureWorkerCommandClient({ executable: '/fixture/az', azureConfigDirectory: root, privateDirectory: root, run: async (args) => {
            if (args[0] === 'rest') {
                puts++;
                const body = JSON.parse(readFileSync(args[args.indexOf('--body') + 1]!.slice(1), 'utf8'));
                expect(body.properties.asyncExecution).toBe(true);
                expect(args.join(' ')).not.toContain('fixture-worker-bearer');
                const request = JSON.parse(Buffer.from(body.properties.protectedParameters[0].value, 'base64').toString());
                let output = JSON.stringify({ version: 1, installed: true, manifestDigest });
                if (request.operation === 'prepare') {
                    controllerKey = request.payload.controllerPublicKeyPem;
                    output = JSON.stringify({ version: 1, binding: request.payload.binding, publicKeyPem: key.publicKey.export({ format: 'pem', type: 'spki' }).toString() });
                }
                if (request.operation === 'enroll')
                    output = sealEnrollment(Buffer.from('fixture-worker-bearer-'.repeat(3)), request.payload.binding, createPublicKey(controllerKey));
                commands.set(args[args.indexOf('--url') + 1]!.split('/runCommands/')[1]!.split('?')[0]!, { output, digest: body.tags.openrigRequest });
                if (unknown)
                    throw Error('fixture-worker-bearer-must-not-escape');
                return '';
            }
            if (args[1] === 'run-command') {
                if (unknown)
                    throw Error('not-found-is-not-nondelivery');
                const name = args[args.indexOf('--name') + 1]!;
                const saved = commands.get(name)!;
                return JSON.stringify({ id: `${instance.resourceId}/runCommands/${name}`, name, tags: { openrigRequest: saved.digest }, instanceView: { executionState: 'Succeeded', exitCode: 0, output: saved.output + '\n' } });
            }
            const imageReference = { publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' };
            if (args[1] === 'nic')
                return JSON.stringify([{ id: `${instance.resourceId}/networkInterfaces/nic`, virtualMachine: { id: instance.resourceId }, ipConfigurations: [{ privateIPAddress: '10.0.0.2', privateIPAddressVersion: 'IPv4' }] }]);
            if (args.includes('--instance-id'))
                return JSON.stringify({ id: instance.resourceId, instanceId: '0', vmId, protectionPolicy: { protectFromScaleIn: true }, storageProfile: { imageReference }, networkProfile: { networkInterfaces: [{ id: `${instance.resourceId}/networkInterfaces/nic` }] } });
            return JSON.stringify({ id: resourceId, location: 'fixture-region', tags: { pool: 'fixture' }, virtualMachineProfile: { storageProfile: { imageReference } } });
        } });
    const registryPath = join(root, 'hosts.yaml');
    writeFileSync(registryPath, 'hosts:\n  - id: permanent\n    transport: http\n    url: http://192.168.0.2:7433\n', { mode: 0o600 });
    const options = { store, commands: client, manifestRaw, preparationPlan, fleetEpoch: 'fixture-epoch', workerBuildCommit: 'c'.repeat(40), artifact: { version: 1 as const, url: 'https://10.0.0.1:8443/api/elastic-artifacts/' + 'd'.repeat(64), privateIPv4: '10.0.0.1', archiveSha256: 'd'.repeat(64), archiveBytes: 123, caPem: 'fixture', caSha256: 'e'.repeat(64) }, packageRecipe: { distribution: 'ubuntu' as const, release: '24.04' as const, architecture: 'amd64' as const, basePackages: { apt: ['2.8.3'] }, packages: [], finalTools: [], signedMetadata: { keyringFile: 'fixture', keyringSha256: 'e'.repeat(64), releases: [] } }, checkpointCaPem: 'fixture', stageScript: script, helperScript: script, registryPath, now: () => 10000,
        fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
            expect(init?.redirect).toBe('error');
            expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer ' + 'fixture-worker-bearer-'.repeat(3));
            if (init?.method === 'POST') {
                providerReady = true;
                return new Response('{}');
            }
            return new Response(JSON.stringify({ version: 1, observedAt: 10000, selfHostId: 'fixture-native-host', build: { commit: 'c'.repeat(40), dirty: false }, state: { version: 1, workerId: instance.workerId, vmId, manifestDigest, phase: 'ready', generation: 0, drainOperationId: null, drainStartedAt: null }, ledger: { complete: true, custody: 7, runnable: 0, claims: 0, unresolvedOutbox: 0, pendingHumanReturns: false, pendingActivationIntents: false, pendingTransfers: false, clean: false, digest: 'a'.repeat(64) }, filesystem: { complete: true, blockers: [], files: [], refs: [], digest: 'a'.repeat(64) }, process: { observedAt: 10000, complete: !busy, idle: !busy, processes: 7, uid: 490, cgroup: '/system.slice/openrig-elastic.service' }, checkpointAcknowledged: false, clean: false, drainEligible: false, preparation: { version: 1, observedAt: 10000, manifestDigest, operationId: preparationPlan.operationId, planDigest: createHash('sha256').update(JSON.stringify(preparationPlan)).digest('hex'), state: providerReady ? 'ready' : 'pending', rung: providerReady ? 'tool-proven' : 'enrolled', rigId: providerReady ? 'fixture-rig' : null, seats: providerReady ? preparationPlan.seats.map(s => ({ sessionName: s.sessionName, repositoryId:s.repository.id, nodeId: s.sessionName, generation: 'fixture-generation', cwd: '/fixture/work', runtime: 'pi', model: preparationPlan.model, launched: true, providerReady: true, toolProven: true, active: busy && s.sessionName==='worker-a@elastic' })) : [], repositoryCapabilities: providerReady ? [{ repositoryId: 1, publicRead: true, privateRead: false, write: false }] : [], publicReason: 'fixture-native', capacity:providerReady?capacity():undefined } }));
        } };
    return { context, options, store, puts: () => puts, registryPath, setBusy:()=>{busy=true;} };
}
it('concretely stages, wraps enrollment, registers without touching permanent hosts and gates readiness', async () => {
    const f = fixture();
    const service = new ElasticWorkerEnrollment(f.options);
    expect((await service.advanceEnrollment(f.context)).state).toBe('pending');
    expect(await service.inspectNative({ configuration: f.context.configuration, instances: [f.context.instance], operation: f.context.operation })).toBe('pending');
    expect(f.puts()).toBe(1);
    expect((await service.advanceEnrollment(f.context)).state).toBe('pending');
    expect((await service.advanceEnrollment(f.context)).state).toBe('pending');
    expect((await service.advanceEnrollment(f.context)).state).toBe('pending');
    expect(f.puts()).toBe(3);
    expect((await service.advanceEnrollment(f.context)).state).toBe('ready');
    const loaded = loadHostRegistry(f.registryPath);
    expect(loaded.ok && loaded.registry.hosts.map(h => h.id)).toContain('permanent');
    expect((await service.inspectEnrollment(f.context)).state).toBe('ready');
    expect(f.puts()).toBe(3);
    f.setBusy();
    let clock = 10000;
    f.options.now = () => clock;
    const originalFetch = f.options.fetchImpl;
    f.options.fetchImpl = async (input, init) => {
        const response = await originalFetch(input, init);
        const raw = await response.json(); clock = 10001; raw.observedAt = clock;
        return new Response(JSON.stringify(raw));
    };
    const observed=await service.observeManagedWorker(f.store.list()[0]!,f.context.configuration);
    expect(observed.worker.state).toBe('ready');
    expect(observed.worker.active).toBe(1);
    expect(observed.probe?.preparation.capacity?.seats.map(s=>s.occupancy)).toEqual(['occupied','free']);
    expect(observed.probe?.process.complete).toBe(false);
    expect(observed.drainEligible).toBe(false);
});
it('retains original key/request after unknown PUT and missing GET across restart without another submission', async () => {
    const f = fixture(true);
    const service = new ElasticWorkerEnrollment(f.options);
    const first = await service.advanceEnrollment(f.context);
    expect(first.state).toBe('unknown');
    const a = f.store.listPending()[0]!;
    const key = f.store.privateKey(a.id);
    const restarted = new ElasticWorkerEnrollment(f.options);
    expect((await restarted.advanceEnrollment(f.context)).state).toBe('unknown');
    expect((await restarted.inspectEnrollment(f.context)).state).toBe('unknown');
    expect(f.puts()).toBe(1);
    expect(f.store.privateKey(a.id)).toEqual(key);
    expect(JSON.stringify(first)).not.toContain('bearer');
});

it('requires exactly one ready attempt per added incarnation after restart without submitting commands', async () => {
    const f = fixture();
    const service = new ElasticWorkerEnrollment(f.options);
    for (let step = 0; step < 5; step++)
        await service.advanceEnrollment(f.context);
    const saved = f.store.list()[0]!;
    const restarted = new ElasticWorkerEnrollment(f.options);
    const inspect = (instances: typeof f.context.instance[], operation: FleetOperation = f.context.operation) =>
        restarted.inspectNative({ configuration: f.context.configuration, instances, operation });
    expect(await inspect([f.context.instance])).toBe('succeeded');
    const second = { ...f.context.instance, instanceId: '1', vmId: '22222222-2222-4222-8222-222222222222', resourceId: `${f.context.configuration.resourceId}/virtualMachines/1`, workerId: `azure:${f.context.configuration.resourceId.toLowerCase()}:22222222-2222-4222-8222-222222222222` };
    const observation = await restarted.observeManagedWorker(saved, f.context.configuration);
    const withBaseline = { ...f.context.operation, targetCapacity: 2,
        snapshot: { ...f.context.operation.snapshot, workers: [{ ...observation.worker, id: second.workerId }] } };
    expect(await inspect([f.context.instance, second], withBaseline)).toBe('succeeded');
    expect(await inspect([f.context.instance, { ...second, workerId: 'changed-incarnation' }], withBaseline)).toBe('unknown');
    const expanded = { ...f.context.operation, count: 2, targetCapacity: 2 };
    expect(await inspect([f.context.instance, second], expanded)).toBe('unknown');
    expect(await inspect([f.context.instance, f.context.instance], expanded)).toBe('unknown');
    for (const changed of [
        { ...f.context.instance, instanceId: '1' },
        { ...f.context.instance, resourceId: second.resourceId },
        { ...f.context.instance, vmId: second.vmId },
        second,
    ])
        expect(await inspect([changed])).toBe('unknown');
    const extra = f.store.createOrRead({ ...saved.identity, instanceId: second.instanceId, childResourceId: second.resourceId, vmId: second.vmId });
    expect(extra.phase).toBe('reserved');
    const before = f.store.list();
    expect(await inspect([f.context.instance])).toBe('unknown');
    expect(await inspect([f.context.instance, second], withBaseline)).toBe('unknown');
    expect(f.store.list()).toEqual(before);
    expect(f.puts()).toBe(3);
});

import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { SessionRegistry } from '../src/domain/session-registry.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { EventBus } from '../src/domain/event-bus.js';
import { Hono } from 'hono';
import { queueRoutes } from '../src/routes/queue.js';
import { bindCoordinatorReadinessPlan, ElasticEnrolledWorkerDirectory } from '../src/adapters/elastic-coordinator-admission.js';
import { ElasticFleetProjection } from '../src/domain/elastic-fleet-projection.js';
import { readinessTaskBody } from '../src/adapters/elastic-worker-prepare.js';
it('delivers two stable real-owner native readiness tasks only after exact registration and reconciles an unknown create without duplicate',async()=>{
 const f=fixture();const db=createDb(join(f.options.store.root,'native.sqlite'));migrate(db,ALL_MIGRATIONS);
 try {
  db.prepare("INSERT INTO rigs(id,name) VALUES('owner-rig','coordinator')").run();db.prepare("INSERT INTO nodes(id,rig_id,logical_id,role,runtime) VALUES('owner-node','owner-rig','manager','manager','pi')").run();const sessions=new SessionRegistry(db);sessions.registerSession('owner-node','manager@coordinator');
  expect(()=>bindCoordinatorReadinessPlan({plan:f.options.preparationPlan,sessions,manager:'invented@coordinator',hostId:'coordinator'})).toThrow('owner_required');
  Object.assign(f.options.preparationPlan,bindCoordinatorReadinessPlan({plan:f.options.preparationPlan,sessions,manager:'manager@coordinator',hostId:'coordinator'}));
  const queue=new QueueRepository(db,new EventBus(db),{validateRig:()=>true});const app=new Hono();app.use('*',async(c,next)=>{c.set('queueRepo' as never,queue as never);await next();});app.route('/api/queue',queueRoutes());
  let creates=0,lost=true;const original=f.options.fetchImpl;
  f.options.fetchImpl=async(input,init)=>{const url=new URL(String(input));if(url.pathname.startsWith('/api/queue/')){expect(loadHostRegistry(f.registryPath).ok).toBe(true);const response=await app.request(url.pathname,{...init});if(url.pathname.endsWith('/create')){creates++;if(lost){lost=false;throw Error('lost-native-create-ack');}}return response;}return original(input,init);};
  const enrollment=new ElasticWorkerEnrollment(f.options);await enrollment.advanceEnrollment(f.context);await enrollment.advanceEnrollment(f.context);await enrollment.advanceEnrollment(f.context);await enrollment.advanceEnrollment(f.context);
  const directory=new ElasticEnrolledWorkerDirectory({store:f.store,enrollment,configuration:()=>f.context.configuration,sessions,coordinatorManager:'manager@coordinator',coordinatorHuman:'human@kernel',originHuman:'operator@external',projection:new ElasticFleetProjection(db),fetchImpl:f.options.fetchImpl});
  const attempt=f.store.list()[0]!;await directory.readiness(attempt.id);await directory.readiness(attempt.id);
  expect(creates).toBe(2);expect(queue.list({limit:10})).toHaveLength(2);
  const plan=enrollment.preparationPlan();expect(plan.owner!.tasks).toHaveLength(2);
  for(const task of plan.owner!.tasks){const actual=queue.getById(task.qitemId)!;expect(actual.sourceSession).toBe('manager@coordinator@coordinator');expect(actual.destinationSession).toBe(task.sessionName);expect(actual.body).toBe(readinessTaskBody({qitemId:task.qitemId,nonce:task.nonce,sourceSession:'manager@coordinator',originHostId:'coordinator'},task.sessionName,enrollment.workerWorkspace(task.sessionName),plan.manifestDigest));expect(actual.chainOfRecord).toContain(task.qitemId);}
  expect((await enrollment.inspectEnrollment(f.context)).state).toBe('pending');
 } finally {db.close();}
});
