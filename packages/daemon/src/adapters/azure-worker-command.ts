import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readFileSync, realpathSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';
import { z } from 'zod';
import { CheckpointPrivateStore } from './elastic-checkpoint-private-store.js';
import { azureWorkerId, validateAzurePoolConfiguration, type AzureArgvRunner, type AzureInstance, type AzurePoolConfiguration } from './azure-elastic-fleet.js';
import { privateIPv4, type TrustedInstance } from './linux-privileged-worker-boundary.js';
import type { EnrollmentBinding } from '../domain/elastic-enrollment.js';
import { assertManagedCommandSuccess } from './azure-worker-bootstrap.js';
import { enrollmentDigest, type ElasticWorkerAttempt } from './elastic-worker-attempts.js';
const object = z.record(z.string(), z.unknown());
const imageSchema = z.object({ id: z.string().optional(), publisher: z.string().optional(), offer: z.string().optional(), sku: z.string().optional(), version: z.string().optional() }).passthrough();
function reject(): never { throw Error('azure_worker_command_rejected'); }
function same(a: string, b: string): boolean { return a.toLowerCase() === b.toLowerCase(); }
function imageMatches(image: z.infer<typeof imageSchema>, expected: AzurePoolConfiguration['image']): boolean {
    return expected.kind === 'id' ? typeof image.id === 'string' && same(image.id, expected.id) : image.publisher === expected.publisher && image.offer === expected.offer && image.sku === expected.sku && image.version === expected.version;
}
export interface TrustedWorkerEndpoint {
    instance: TrustedInstance;
    baseUrl: string;
    location: string;
}
export interface AzureWorkerCommandOptions {
    executable: string;
    azureConfigDirectory: string;
    privateDirectory: string;
    run?: AzureArgvRunner;
}
import type { ManagedWorkerOperation } from './elastic-managed-operation.js';
export { managedWorkerOperations, type ManagedWorkerOperation } from './elastic-managed-operation.js';
export interface PinnedWorkerScript {
    path: string;
    sha256: string;
}
export interface ManagedWorkerInput {
    configuration: AzurePoolConfiguration;
    endpoint: TrustedWorkerEndpoint;
    script: PinnedWorkerScript;
    payload: unknown;
    /** Native production owner/configuration recheck after asynchronous ARM preparation. */
    assertDispatch?: () => void;
}
export type ManagedCommandObservation = {
    state: 'pending' | 'unknown' | 'failed';
} | {
    state: 'succeeded';
    output: string;
};
/** Optional CLI dependency. Production never interpolates a shell command or inherits an account token. */
export class AzureWorkerCommandClient {
    constructor(private readonly options: AzureWorkerCommandOptions) {
        if (![options.executable, options.azureConfigDirectory, options.privateDirectory].every(p => isAbsolute(p) && !p.includes('\0')))
            reject();
    }
    async run(args: readonly string[], beforeDispatch?: () => void): Promise<unknown> {
        try {
            const limits = { timeout: 30000, maxBuffer: 1024 * 1024, ...(beforeDispatch ? { beforeDispatch } : {}) };
            const argv = [...args, '--only-show-errors', '--output', 'json'];
            const output = this.options.run ? await this.options.run(argv, limits) : await new Promise<string>((resolve, rejectPromise) => {
                beforeDispatch?.();
                execFile(this.options.executable, argv, { timeout: limits.timeout, maxBuffer: limits.maxBuffer, encoding: 'utf8', shell: false,
                    env: { PATH: '/usr/bin:/bin', HOME: this.options.azureConfigDirectory, AZURE_CONFIG_DIR: this.options.azureConfigDirectory, AZURE_CORE_COLLECT_TELEMETRY: '0', AZURE_EXTENSION_USE_DYNAMIC_INSTALL: 'no' } }, (error, stdout) => error ? rejectPromise(Error('azure_worker_command_unknown')) : resolve(stdout));
            });
            if (Buffer.byteLength(output) > limits.maxBuffer)
                reject();
            return output.trim() ? JSON.parse(output) : null;
        }
        catch {
            throw Error('azure_worker_command_unknown');
        }
    }
    async resolveTrustedInstance(config: AzurePoolConfiguration, instance: AzureInstance, binding: EnrollmentBinding, manifest: {
        port: number;
        imageReference: string;
    }): Promise<TrustedWorkerEndpoint> {
        const c = validateAzurePoolConfiguration(config, config.poolId);
        if (binding.resourceId !== c.resourceId || binding.vmId !== instance.vmId || instance.workerId !== azureWorkerId(c.resourceId, instance.vmId)
            || !/^\d{1,10}$/.test(instance.instanceId) || !same(instance.resourceId, `${c.resourceId}/virtualMachines/${instance.instanceId}`) || !instance.protected || !Number.isInteger(manifest.port) || manifest.port < 1024 || manifest.port > 65535)
            reject();
        const common = ['--subscription', c.subscription, '--resource-group', c.resourceGroup];
        const model = z.object({ id: z.string(), location: z.string().regex(/^[a-zA-Z0-9-]{1,64}$/), tags: z.record(z.string(), z.string()), virtualMachineProfile: z.object({ storageProfile: z.object({ imageReference: imageSchema }) }) }).passthrough().parse(await this.run(['vmss', 'show', '--name', c.scaleSetName, ...common]));
        const child = z.object({ id: z.string(), instanceId: z.string(), vmId: z.string().uuid(), protectionPolicy: z.object({ protectFromScaleIn: z.literal(true) }), storageProfile: z.object({ imageReference: imageSchema }), networkProfile: z.object({ networkInterfaces: z.array(z.object({ id: z.string() }).passthrough()).min(1).max(8) }) }).passthrough().parse(await this.run(['vmss', 'show', '--name', c.scaleSetName, '--instance-id', instance.instanceId, ...common]));
        // `vmss show --instance-id` returns the child, not the scale-set model.
        if (!same(model.id, c.resourceId) || model.tags[c.ownershipTag.key] !== c.ownershipTag.value || !imageMatches(model.virtualMachineProfile.storageProfile.imageReference, c.image)
            || !same(child.id, instance.resourceId) || child.instanceId !== instance.instanceId || child.vmId !== binding.vmId || !imageMatches(child.storageProfile.imageReference, c.image))
            reject();
        const expectedImage = c.image.kind === 'id' ? c.image.id : `${c.image.publisher}:${c.image.offer}:${c.image.sku}:${c.image.version}`;
        if (manifest.imageReference !== expectedImage)
            reject();
        const nics = z.array(z.object({ id: z.string(), virtualMachine: z.object({ id: z.string() }), ipConfigurations: z.array(z.object({ privateIPAddress: z.string(), privateIPAddressVersion: z.string().optional(), publicIPAddress: z.unknown().optional() }).passthrough()).max(16) }).passthrough()).max(8).parse(await this.run(['vmss', 'nic', 'list-vm-nics', '--vmss-name', c.scaleSetName, '--instance-id', instance.instanceId, ...common]));
        if (nics.length !== child.networkProfile.networkInterfaces.length || nics.some(n => !same(n.virtualMachine.id, child.id) || !child.networkProfile.networkInterfaces.some(ref => same(ref.id, n.id))))
            reject();
        const addresses = nics.flatMap(n => n.ipConfigurations).filter(ip => ip.privateIPAddressVersion === undefined || ip.privateIPAddressVersion === 'IPv4');
        if (addresses.length !== 1 || !privateIPv4(addresses[0]!.privateIPAddress) || addresses.some(ip => ip.publicIPAddress != null))
            reject();
        const trusted: TrustedInstance = { version: 1, imageReference: manifest.imageReference, resourceId: c.resourceId, vmId: child.vmId, manifestDigest: binding.manifestDigest, fleetEpoch: binding.fleetEpoch, operationId: binding.operationId, privateIPv4: addresses[0]!.privateIPAddress };
        return { instance: trusted, baseUrl: `http://${trusted.privateIPv4}:${manifest.port}`, location: model.location };
    }
    commandIdentity(attempt: ElasticWorkerAttempt, operation: ManagedWorkerOperation, payload: unknown, script: PinnedWorkerScript) {
        const bytes = this.script(script);
        const requestDigest = enrollmentDigest({ binding: attempt.binding, operation, payload, scriptDigest: script.sha256 });
        const commandNames: Partial<Record<ManagedWorkerOperation, string>> = {
            'github-credential-install': 'github', 'final-checkpoint-export': 'export',
            'final-checkpoint-upload': 'upload', 'final-checkpoint-ack': 'ack',
        };
        return { name: `openrig-${commandNames[operation] ?? operation}-${requestDigest.slice(0, 40)}`, requestDigest, bytes };
    }
    private script(script: PinnedWorkerScript): string {
        if (!isAbsolute(script.path) || !/^[a-f0-9]{64}$/.test(script.sha256))
            reject();
        const stat = lstatSync(script.path);
        if (!stat.isFile() || stat.nlink !== 1 || realpathSync(script.path) !== script.path || stat.size > 128 * 1024 || (stat.mode & 0o022))
            reject();
        const bytes = readFileSync(script.path, 'utf8');
        if (createHash('sha256').update(bytes).digest('hex') !== script.sha256 || bytes.includes('\nOPENRIG_FIXED_SCRIPT'))
            reject();
        return bytes;
    }
    async submitManagedCommand(attempt: ElasticWorkerAttempt, operation: ManagedWorkerOperation, input: ManagedWorkerInput): Promise<'submitted' | 'unknown'> {
        const fresh = await this.resolveTrustedInstance(input.configuration, { resourceId: attempt.identity.childResourceId, instanceId: attempt.identity.instanceId, vmId: attempt.binding.vmId, workerId: attempt.workerId, protected: true, provisioningState: 'Succeeded' }, attempt.binding, { port: Number(new URL(input.endpoint.baseUrl).port), imageReference: input.endpoint.instance.imageReference });
        if (enrollmentDigest(fresh) !== enrollmentDigest(input.endpoint))
            reject();
        const identity = this.commandIdentity(attempt, operation, input.payload, input.script);
        if (!attempt.command || attempt.command.name !== identity.name || attempt.command.requestDigest !== identity.requestDigest || attempt.command.operation !== operation || attempt.command.state !== 'intent')
            reject();
        const parent = new CheckpointPrivateStore(this.options.privateDirectory);
        const commandDirectory = join(parent.root, 'managed-commands');
        if (!existsSync(commandDirectory))
            mkdirSync(commandDirectory, { mode: 0o700 });
        parent.sync(parent.root);
        const store = new CheckpointPrivateStore(commandDirectory);
        const dir = store.child(identity.requestDigest);
        const request = JSON.stringify({ location: input.endpoint.location, tags: { openrigRequest: identity.requestDigest }, properties: {
                asyncExecution: true, timeoutInSeconds: operation === 'stage' ? 900 : 180,
                source: { script: `#!/bin/sh\nset -eu\nexec /usr/bin/python3 - <<'OPENRIG_FIXED_SCRIPT'\n${identity.bytes}\nOPENRIG_FIXED_SCRIPT\n` },
                protectedParameters: [{ name: 'OPENRIG_REQUEST', value: Buffer.from(JSON.stringify({ version: 1, operation, instance: input.endpoint.instance, payload: input.payload, requestDigest: identity.requestDigest })).toString('base64') }],
            } });
        const path = join(dir, 'protected.json');
        if (existsSync(path)) {
            if (store.read(path, 256 * 1024).toString() !== request)
                reject();
        }
        else
            store.write(path, request);
        // CLI 2.90's create wrapper does not support a protected-parameter JSON file.
        // This is the same managed CreateOrUpdate API, with the entire body kept private.
        const url = `https://management.azure.com${attempt.identity.childResourceId}/runCommands/${identity.name}?api-version=2024-11-01`;
        try {
            input.assertDispatch?.();
            await this.run(['rest', '--method', 'put', '--url', url, '--body', `@${path}`], input.assertDispatch);
            return 'submitted';
        }
        catch {
            return 'unknown';
        }
    }
    async inspectManagedCommand(attempt: ElasticWorkerAttempt, configuration: AzurePoolConfiguration): Promise<ManagedCommandObservation> {
        if (!attempt.command)
            return { state: 'unknown' };
        try {
            const result = z.object({ id: z.string(), name: z.string(), tags: z.record(z.string(), z.string()), instanceView: z.object({ executionState: z.string(), exitCode: z.number().optional(), output: z.string().optional() }).passthrough().optional() }).passthrough().parse(await this.run(['vmss', 'run-command', 'show', '--subscription', configuration.subscription, '--resource-group', configuration.resourceGroup, '--vmss-name', configuration.scaleSetName,
                '--instance-id', attempt.identity.instanceId, '--name', attempt.command.name, '--instance-view']));
            if (!same(result.id, `${attempt.identity.childResourceId}/runCommands/${attempt.command.name}`) || result.name !== attempt.command.name || result.tags.openrigRequest !== attempt.command.requestDigest)
                reject();
            const view = result.instanceView;
            if (!view || ['Pending', 'Running', 'Unknown'].includes(view.executionState))
                return { state: 'pending' };
            if (view.executionState !== 'Succeeded' || view.exitCode !== 0)
                return { state: 'failed' };
            return { state: 'succeeded', output: assertManagedCommandSuccess(view) };
        }
        catch {
            return { state: 'unknown' };
        }
    }
}
