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
            return new Response(JSON.stringify({ version: 1, observedAt: 10000, selfHostId: 'fixture-native-host', build: { commit: 'c'.repeat(40), dirty: false }, state: { version: 1, workerId: instance.workerId, vmId, manifestDigest, phase: 'ready', generation: 0, drainOperationId: null, drainStartedAt: null }, ledger: { complete: true, custody: 7, runnable: 0, claims: 0, unresolvedOutbox: 0, pendingHumanReturns: false, pendingActivationIntents: false, pendingTransfers: false, clean: false, digest: 'a'.repeat(64) }, filesystem: { complete: true, blockers: [], files: [], refs: [], digest: 'a'.repeat(64) }, process: { observedAt: 10000, complete: true, idle: true, processes: 7, uid: 490, cgroup: '/system.slice/openrig-elastic.service' }, checkpointAcknowledged: false, clean: false, drainEligible: false, preparation: { version: 1, observedAt: 10000, manifestDigest, operationId: preparationPlan.operationId, planDigest: createHash('sha256').update(JSON.stringify(preparationPlan)).digest('hex'), state: providerReady ? 'ready' : 'pending', rung: providerReady ? 'tool-proven' : 'enrolled', rigId: providerReady ? 'fixture-rig' : null, seats: providerReady ? preparationPlan.seats.map(s => ({ sessionName: s.sessionName, nodeId: s.sessionName, generation: 'fixture-generation', cwd: '/fixture/work', runtime: 'pi', model: preparationPlan.model, launched: true, providerReady: true, toolProven: true, active: false })) : [], repositoryCapabilities: providerReady ? [{ repositoryId: 1, publicRead: true, privateRead: false, write: false }] : [], publicReason: 'fixture-native' } }));
        } };
    return { context, options, store, puts: () => puts, registryPath };
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
