import { afterEach, describe, expect, it } from 'vitest';
import { chmodSync, mkdtempSync, realpathSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { AzureWorkerCommandClient } from '../src/adapters/azure-worker-command.js';
const roots: string[] = [];
afterEach(() => roots.splice(0).forEach(p => rmSync(p, { recursive: true, force: true })));
const resourceId = '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture';
const configuration = { kind: 'azure-vmss' as const, subscription: '00000000-0000-0000-0000-000000000001', resourceGroup: 'fixture', scaleSetName: 'fixture', resourceId, poolId: 'fixture', ownershipTag: { key: 'pool', value: 'fixture' }, image: { kind: 'marketplace' as const, publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' }, bootstrap: { path: '/fixture', digest: 'a'.repeat(64) } };
const vmId = '11111111-1111-4111-8111-111111111111';
const instance = { resourceId: `${resourceId}/virtualMachines/0`, instanceId: '0', vmId, workerId: `azure:${resourceId.toLowerCase()}:${vmId}`, protected: true, provisioningState: 'Succeeded' };
const binding = { version: 1 as const, resourceId, vmId, nonce: Buffer.alloc(32).toString('base64'), manifestDigest: 'a'.repeat(64), operationId: 'fixture', fleetEpoch: 'epoch' };
describe('trusted ARM and managed commands', () => {
    it('uses the actual configured argv runner with isolated Azure config and no shell', async () => {
        const root = realpathSync(mkdtempSync(join(tmpdir(), 'azure-cli-')));
        roots.push(root);
        chmodSync(root, 0o700);
        const executable = join(root, 'az');
        const log = join(root, 'argv.json');
        writeFileSync(executable, `#!${process.execPath}\nrequire('fs').writeFileSync(${JSON.stringify(log)},JSON.stringify({argv:process.argv.slice(2),config:process.env.AZURE_CONFIG_DIR})); process.stdout.write('{"fixture":true}');`);
        chmodSync(executable, 0o700);
        const cli = new AzureWorkerCommandClient({ executable, azureConfigDirectory: root, privateDirectory: root });
        expect(await cli.run(['version'])).toEqual({ fixture: true });
        expect(JSON.parse(readFileSync(log, 'utf8'))).toEqual({ argv: ['version', '--only-show-errors', '--output', 'json'], config: root });
    });
    it('requires exact image/tag/incarnation/protection and a unique attached private NIC', async () => {
        let ips = ['10.0.0.2'];
        const cli = new AzureWorkerCommandClient({ executable: '/fixture/az', azureConfigDirectory: '/fixture', privateDirectory: '/fixture', run: async (args) => {
                if (args[0] === 'vmss' && args[1] === 'show' && !args.includes('--instance-id'))
                    return JSON.stringify({ id: resourceId, location: 'fixture-region', tags: { pool: 'fixture' }, virtualMachineProfile: { storageProfile: { imageReference: { publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' } } } });
                if (args[0] === 'vmss' && args[1] === 'get-instance-view')
                    throw Error('unexpected');
                if (args[0] === 'vmss' && args[1] === 'nic')
                    return JSON.stringify([{ id: `${instance.resourceId}/networkInterfaces/nic`, virtualMachine: { id: instance.resourceId }, ipConfigurations: ips.map(ip => ({ privateIPAddress: ip, privateIPAddressVersion: 'IPv4' })) }]);
                return JSON.stringify({ id: instance.resourceId, instanceId: '0', vmId, protectionPolicy: { protectFromScaleIn: true }, storageProfile: { imageReference: { publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' } }, networkProfile: { networkInterfaces: [{ id: `${instance.resourceId}/networkInterfaces/nic` }] } });
            } });
        expect((await cli.resolveTrustedInstance(configuration, instance, binding, { port: 7433, imageReference: 'Canonical:ubuntu:24:1.2.3' })).baseUrl).toBe('http://10.0.0.2:7433');
        ips = ['10.0.0.2', '10.0.0.3'];
        await expect(cli.resolveTrustedInstance(configuration, instance, binding, { port: 7433, imageReference: 'Canonical:ubuntu:24:1.2.3' })).rejects.toThrow();
    });
});
import { createHash } from 'node:crypto';
import { ElasticWorkerAttemptStore } from '../src/adapters/elastic-worker-attempts.js';
it('default execFile submits a private ARM body and reads the same managed command after a timeout', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'managed-argv-')));
    roots.push(root);
    chmodSync(root, 0o700);
    const executable = join(root, 'az');
    const log = join(root, 'argv.json');
    const script = join(root, 'fixed.py');
    writeFileSync(script, 'print("fixture")\n', { mode: 0o600 });
    const imageReference = { publisher: 'Canonical', offer: 'ubuntu', sku: '24', version: '1.2.3' };
    const model = { id: resourceId, location: 'fixture-region', tags: { pool: 'fixture' }, virtualMachineProfile: { storageProfile: { imageReference } } };
    const child = { id: instance.resourceId, instanceId: '0', vmId, protectionPolicy: { protectFromScaleIn: true }, storageProfile: { imageReference }, networkProfile: { networkInterfaces: [{ id: `${instance.resourceId}/networkInterfaces/nic` }] } };
    const nics = [{ id: `${instance.resourceId}/networkInterfaces/nic`, virtualMachine: { id: instance.resourceId }, ipConfigurations: [{ privateIPAddress: '10.0.0.2', privateIPAddressVersion: 'IPv4' }] }];
    writeFileSync(executable, `#!${process.execPath}\nconst fs=require('fs');const a=process.argv.slice(2);if(a[0]==='rest'){const p=a[a.indexOf('--body')+1].slice(1);const body=JSON.parse(fs.readFileSync(p));fs.writeFileSync(${JSON.stringify(log)},JSON.stringify({args:a,mode:fs.statSync(p).mode&511,request:JSON.parse(Buffer.from(body.properties.protectedParameters[0].value,'base64')),body}));process.stderr.write('fixture-secret-do-not-report');process.exit(1)}if(a[1]==='run-command'){process.stdout.write(JSON.stringify({provisioningState:'Succeeded'}));}else process.stdout.write(JSON.stringify(a[1]==='nic'?${JSON.stringify(nics)}:a.includes('--instance-id')?${JSON.stringify(child)}:${JSON.stringify(model)}));`);
    chmodSync(executable, 0o700);
    const client = new AzureWorkerCommandClient({ executable, azureConfigDirectory: root, privateDirectory: root });
    const store = new ElasticWorkerAttemptStore(root);
    const a = store.createOrRead({ resourceId, childResourceId: instance.resourceId, instanceId: '0', vmId, operationId: binding.operationId, fleetEpoch: binding.fleetEpoch, manifestDigest: binding.manifestDigest, artifactDigest: 'b'.repeat(64), specDigest: 'c'.repeat(64), checkoutDigest: 'd'.repeat(64), configurationDigest: 'e'.repeat(64) });
    const pinned = { path: script, sha256: createHash('sha256').update(readFileSync(script)).digest('hex') };
    const payload = { binding: a.binding, materialEnvelope: 'fixture-ciphertext' };
    const command = client.commandIdentity(a, 'enroll', payload, pinned);
    const pending = store.advance(a.id, 0, { command: { name: command.name, requestDigest: command.requestDigest, operation: 'enroll', state: 'intent' } });
    const endpoint = await client.resolveTrustedInstance(configuration, instance, a.binding, { port: 7433, imageReference: 'Canonical:ubuntu:24:1.2.3' });
    expect(await client.submitManagedCommand(pending, 'enroll', { configuration, endpoint, script: pinned, payload })).toBe('unknown');
    const captured = JSON.parse(readFileSync(log, 'utf8'));
    expect(captured.mode).toBe(0o600);
    expect(captured.args).toContain(`https://management.azure.com${instance.resourceId}/runCommands/${command.name}?api-version=2024-11-01`);
    expect(captured.args.join(' ')).not.toContain('fixture-ciphertext');
    expect(captured.body.properties.asyncExecution).toBe(true);
    expect(captured.request.operation).toBe('enroll');
    expect(captured.request.instance.vmId).toBe(vmId);
    expect(await client.inspectManagedCommand(pending, configuration)).toEqual({ state: 'unknown' });
});

it('persists every finite root phase without exceeding ARM names and retains unknown command identity', () => {
    const root=realpathSync(mkdtempSync(join(tmpdir(),'managed-phases-')));roots.push(root);chmodSync(root,0o700);
    const script=join(root,'fixed.py');writeFileSync(script,'print("fixture")\n',{mode:0o600});
    const client=new AzureWorkerCommandClient({executable:'/fixture',azureConfigDirectory:root,privateDirectory:root});
    const store=new ElasticWorkerAttemptStore(root);
    let a=store.createOrRead({resourceId,childResourceId:instance.resourceId,instanceId:'0',vmId,operationId:binding.operationId,fleetEpoch:binding.fleetEpoch,manifestDigest:binding.manifestDigest,artifactDigest:'b'.repeat(64),specDigest:'c'.repeat(64),checkoutDigest:'d'.repeat(64),configurationDigest:'e'.repeat(64)});
    const pinned={path:script,sha256:createHash('sha256').update(readFileSync(script)).digest('hex')};
    const names=new Set<string>();
    for(const operation of ['begin-drain','quiesce','final-checkpoint-export','final-checkpoint-upload','final-checkpoint-ack','seal-status'] as const) {
      const command=client.commandIdentity(a,operation,{binding:a.binding},pinned);
      expect(command.name).toMatch(/^[a-zA-Z0-9-]{1,64}$/);expect(names.has(command.name)).toBe(false);names.add(command.name);
      a=store.advance(a.id,a.revision,{command:{name:command.name,requestDigest:command.requestDigest,operation,state:'unknown'}});
      const recovered=new ElasticWorkerAttemptStore(root).read(a.id);expect(recovered.command).toEqual(a.command);
      expect(()=>store.advance(a.id,a.revision,{command:null})).toThrow();
      a=store.advance(a.id,a.revision,{command:{...a.command!,state:'succeeded'}});
    }
});

import { parseHelperPayload } from '../src/adapters/elastic-worker-helper.js';
it('requires a sealed capability for the fixed upload command without accepting proof overrides',()=>{
 const binding={workerId:'fixture',vmId:'11111111-1111-4111-8111-111111111111',manifestDigest:'a'.repeat(64),fleetEpoch:'epoch',drainGeneration:1,removalOperationId:'original'};
 expect(parseHelperPayload('final-checkpoint-upload',{binding,capabilityEnvelope:'sealed'})).toEqual({binding,capabilityEnvelope:'sealed'});
 for(const value of [{binding},{binding,capabilityEnvelope:''},{binding,capabilityEnvelope:'sealed',verified:true},{binding:{...binding,drainGeneration:0},capabilityEnvelope:'sealed'}])expect(()=>parseHelperPayload('final-checkpoint-upload',value)).toThrow();
});

it('requires an exact positive final status round and nonce in the root helper DTO',()=>{
 const binding={workerId:'worker',vmId:'11111111-1111-4111-8111-111111111111',manifestDigest:'a'.repeat(64),fleetEpoch:'epoch',drainGeneration:1,removalOperationId:'original-drain'};
 expect(parseHelperPayload('seal-status',{binding,round:1,nonce:'a'.repeat(64)})).toEqual({binding,round:1,nonce:'a'.repeat(64)});
 for(const value of [{binding},{binding,round:0,nonce:'a'.repeat(64)},{binding,round:1,nonce:'short'},{binding,round:1,nonce:'a'.repeat(64),clean:true}]) expect(()=>parseHelperPayload('seal-status',value)).toThrow();
});
