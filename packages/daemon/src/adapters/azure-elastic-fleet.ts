import { performance } from 'node:perf_hooks';
import { execFile } from 'node:child_process';
import type { ElasticFleetConfiguration, FleetIO, FleetMutation, FleetOperation } from '../domain/elastic-fleet.js';
import type { FleetDemand, FleetPolicy, FleetSnapshot, FleetWorkerObservation } from '../domain/elastic-fleet-policy.js';

export interface AzurePoolConfiguration {
  deploymentProfileRef?: string;
  kind: 'azure-vmss';
  subscription: string;
  resourceGroup: string;
  scaleSetName: string;
  resourceId: string;
  poolId: string;
  ownershipTag: { key: string; value: string };
  image: { kind: 'id'; id: string } | { kind: 'marketplace'; publisher: string; offer: string; sku: string; version: string };
  bootstrap: { path: string; digest: string };
}
export interface AzureInstance {
  resourceId: string;
  instanceId: string;
  vmId: string;
  workerId: string;
  protected: boolean;
  provisioningState: string;
}
/** Unknown or incomplete native inventory must reject, including unavailable permanent hosts. */
export interface AzureNativeObservation {
  observedAt: number;
  demands: FleetDemand[];
  permanentWorkers: FleetWorkerObservation[];
  /** Exact ARM coverage. Unknown workers must be explicitly quarantined with unknown telemetry. */
  managedWorkers: FleetWorkerObservation[];
  /** Trusted native adapter derives these from durable pending enrollment, never ARM absence. */
  provisioningAttempts?: Array<{ workerId: string; vmId: string; operationId: string }>;
}
export interface AzureRemovalSeal {
  workerId: string;
  vmId: string;
  operationId: string;
  observedAt: number;
  drainConfirmedAt: number;
  drainGeneration: string;
  beginDrainConfirmed: boolean;
  fenced: boolean;
  active: number;
  claims: number;
  processes: number;
  pendingTransfers: boolean;
  pendingActivationIntents: boolean;
  coordinatorCheckpointAcknowledged: boolean;
  clean: boolean;
  pendingHumanReturns: boolean;
}
export interface AzureNativeContext { configuration: AzurePoolConfiguration; instances: AzureInstance[] }
export interface AzureMutationContext { configuration: AzurePoolConfiguration; operation: FleetMutation; instance: AzureInstance }
export type AzureArgvRunner = (args: readonly string[], limits: { timeout: number; maxBuffer: number }) => Promise<string>;
export interface AzureFleetOptions {
  run?: AzureArgvRunner;
  now?: () => number;
  observeNative: (context: AzureNativeContext) => Promise<AzureNativeObservation>;
  /** Must persist attempt/key before Run Command; verify pinned helper/digest and confirmed native readiness. No coordinator bearer. */
  bootstrap?: (context: AzureMutationContext) => Promise<void>;
  /** Resolves only after a durable native assignment fence and confirmed beginDrain. */
  beginDrain?: (context: AzureMutationContext) => Promise<void>;
  /** Fresh complete custody/process/repository/outbox/human-return proof under the durable drain generation. */
  sealForRemoval?: (context: AzureMutationContext) => Promise<AzureRemovalSeal>;
  retireRegistration?: (context: AzureMutationContext) => Promise<void>;
  /** Read-only durable native attempt/drain/retirement evidence. Never bootstrap or rotate credentials here. */
  inspectNative?: (context: AzureNativeContext & { operation: FleetOperation }) => Promise<'succeeded' | 'pending' | 'unknown'>;
}
const limits = { timeout: 30_000, maxBuffer: 1024 * 1024 };
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const name = /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,89}$/;
function object(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function exact(value: Record<string, unknown>, keys: string[]): boolean { return Object.keys(value).sort().join(',') === [...keys].sort().join(','); }
function integer(value: unknown): value is number { return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0; }
function text(value: unknown): value is string { return typeof value === 'string' && value.length > 0 && value.length <= 512 && !/[\u0000-\u001f]/.test(value); }
function sameId(a: unknown, b: string): boolean { return typeof a === 'string' && a.toLowerCase() === b.toLowerCase(); }
function reject(): never { throw Error('azure_boundary_rejected'); }
export function azureWorkerId(resourceId: string, vmId: string): string { return `azure:${resourceId.toLowerCase()}:${vmId.toLowerCase()}`; }
export function validateAzurePoolConfiguration(value: unknown, poolId: string): AzurePoolConfiguration {
  try {
    if (!object(value) || !exact(value, ['kind','subscription','resourceGroup','scaleSetName','resourceId','poolId','ownershipTag','image','bootstrap',...(Object.hasOwn(value,'deploymentProfileRef')?['deploymentProfileRef']:[])])
      || (Object.hasOwn(value,'deploymentProfileRef') && (typeof value.deploymentProfileRef !== 'string' || !value.deploymentProfileRef.startsWith('/') || value.deploymentProfileRef.includes('\0') || value.deploymentProfileRef.length>4096))
      || value.kind !== 'azure-vmss' || typeof value.subscription !== 'string' || !uuid.test(value.subscription)
      || typeof value.resourceGroup !== 'string' || !name.test(value.resourceGroup) || typeof value.scaleSetName !== 'string' || !name.test(value.scaleSetName)
      || value.poolId !== poolId || !text(poolId)) reject();
    const expected = `/subscriptions/${value.subscription}/resourceGroups/${value.resourceGroup}/providers/Microsoft.Compute/virtualMachineScaleSets/${value.scaleSetName}`;
    if (!sameId(value.resourceId, expected) || !object(value.ownershipTag) || !exact(value.ownershipTag,['key','value'])
      || !text(value.ownershipTag.key) || !name.test(value.ownershipTag.key) || value.ownershipTag.value !== poolId
      || !object(value.bootstrap) || !exact(value.bootstrap,['path','digest']) || typeof value.bootstrap.path !== 'string'
      || !/^\/(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+$/.test(value.bootstrap.path) || value.bootstrap.path.split('/').includes('..')
      || typeof value.bootstrap.digest !== 'string' || !/^[a-f0-9]{64}$/.test(value.bootstrap.digest) || !object(value.image)) reject();
    let image: AzurePoolConfiguration['image'];
    if (value.image.kind === 'id') {
      if (!exact(value.image,['kind','id']) || typeof value.image.id !== 'string'
        || !/^\/subscriptions\/[0-9a-f-]{36}\/resourceGroups\/[A-Za-z0-9_.-]+\/providers\/Microsoft\.Compute\/(?:images\/[A-Za-z0-9_.-]+|galleries\/[A-Za-z0-9_.-]+\/images\/[A-Za-z0-9_.-]+\/versions\/\d+\.\d+\.\d+)$/i.test(value.image.id)) reject();
      image = {kind:'id',id:value.image.id};
    } else {
      if (value.image.kind !== 'marketplace' || !exact(value.image,['kind','publisher','offer','sku','version'])
        || !text(value.image.publisher) || !name.test(value.image.publisher) || !text(value.image.offer) || !name.test(value.image.offer)
        || !text(value.image.sku) || !name.test(value.image.sku) || typeof value.image.version !== 'string' || !/^\d+\.\d+\.\d+$/.test(value.image.version)) reject();
      image = {kind:'marketplace',publisher:value.image.publisher,offer:value.image.offer,sku:value.image.sku,version:value.image.version};
    }
    return {...(typeof value.deploymentProfileRef==='string'?{deploymentProfileRef:value.deploymentProfileRef}:{}),kind:'azure-vmss',subscription:value.subscription,resourceGroup:value.resourceGroup,scaleSetName:value.scaleSetName,resourceId:expected,poolId,
      ownershipTag:{key:value.ownershipTag.key,value:poolId},image,bootstrap:{path:value.bootstrap.path,digest:value.bootstrap.digest}};
  } catch { throw Error('invalid_azure_configuration'); }
}
function matchingImage(value: unknown, expected: AzurePoolConfiguration['image']): boolean {
  if (!object(value)) return false;
  return expected.kind === 'id' ? sameId(value.id, expected.id)
    : value.publisher === expected.publisher && value.offer === expected.offer && value.sku === expected.sku && value.version === expected.version;
}
function imageReference(value: unknown): unknown { return object(value) && object(value.storageProfile) ? value.storageProfile.imageReference : undefined; }
function nativeWorker(value: unknown, now: number, interval: number): FleetWorkerObservation {
  if (!object(value) || !text(value.id) || typeof value.managed !== 'boolean' || typeof value.permanent !== 'boolean'
    || !['ready','provisioning','draining','quarantined'].includes(String(value.state)) || !Array.isArray(value.capabilities) || !value.capabilities.every(text)
    || !(value.slots === null || (integer(value.slots) && value.slots > 0)) || !(value.active === null || integer(value.active))
    || (typeof value.slots === 'number' && typeof value.active === 'number' && value.active > value.slots)
    || !integer(value.observedAt) || value.observedAt > now || now - value.observedAt > interval
    || !(value.idleSince === null || (integer(value.idleSince) && value.idleSince <= value.observedAt))
    || !(value.clean === null || typeof value.clean === 'boolean') || !(value.pendingHumanReturns === null || typeof value.pendingHumanReturns === 'boolean')) reject();
  // Validated external object; whitelist so native diagnostic/credential fields cannot escape.
  return {id:value.id,managed:value.managed,permanent:value.permanent,state:value.state as FleetWorkerObservation['state'],capabilities:[...value.capabilities],
    slots:value.slots,active:value.active,observedAt:value.observedAt,idleSince:value.idleSince,clean:value.clean,pendingHumanReturns:value.pendingHumanReturns};
}
const defaultRun: AzureArgvRunner = (args, bounds) => new Promise((resolve, rejectPromise) => {
  execFile('az', [...args], {encoding:'utf8',timeout:bounds.timeout,maxBuffer:bounds.maxBuffer,windowsHide:true}, (error, stdout) => {
    if (error) rejectPromise(Error('azure_cli_failed')); else resolve(stdout);
  });
});

/** Dedicated VMSS only. An exclusive controller writer is required: Azure exact delete has no incarnation precondition. */
export function createAzureElasticFleetIO(options: AzureFleetOptions): FleetIO {
  const run = options.run ?? defaultRun; const now = options.now ?? Date.now;
  const config = (configuration: ElasticFleetConfiguration) => validateAzurePoolConfiguration(configuration.provider,configuration.policy.poolId);
  async function command(configuration: AzurePoolConfiguration, args: string[], response: 'json' | 'bodyless-delete' = 'json'): Promise<unknown> {
    const stdout = await run([...args,'--subscription',configuration.subscription,'--only-show-errors','--output','json'],limits);
    if (typeof stdout !== 'string' || Buffer.byteLength(stdout) > limits.maxBuffer) reject();
    if (response === 'bodyless-delete' && stdout.trim() === '') return undefined;
    return JSON.parse(stdout);
  }
  async function assertNoAutoscale(configuration: AzurePoolConfiguration): Promise<void> {
    const route = `/subscriptions/${configuration.subscription}/providers/Microsoft.Insights/autoscalesettings`;
    const base = `https://management.azure.com${route}?api-version=2022-10-01`;
    // Only carry the opaque paging token; never execute a provider-supplied URL.
    function pageUrl(value: unknown): string {
      if (typeof value !== 'string' || value.length > 8192 || /[\\\s\u0000-\u001f]/.test(value) || /%(?![0-9a-f]{2})/i.test(value)) reject();
      const parsed = new URL(value);
      const rawPath = value.match(/^https:\/\/management\.azure\.com(\/[^?#]*)/i)?.[1];
      if (parsed.origin !== 'https://management.azure.com' || parsed.username || parsed.password || parsed.hash
        || rawPath?.toLowerCase() !== route.toLowerCase() || parsed.pathname.toLowerCase() !== route.toLowerCase()
        || [...parsed.searchParams.keys()].some(key => key !== 'api-version' && key !== '$skiptoken')
        || parsed.searchParams.getAll('api-version').length > 1
        || (parsed.searchParams.has('api-version') && parsed.searchParams.get('api-version') !== '2022-10-01')
        || parsed.searchParams.getAll('$skiptoken').length !== 1) reject();
      const token = parsed.searchParams.get('$skiptoken');
      if (!token || /[\u0000-\u001f\u007f]/.test(token)) reject();
      return `${base}&${new URLSearchParams({'$skiptoken':token})}`;
    }
    // Inventory names follow Azure's rules, independently of our intentionally narrow pool configuration.
    const resourceGroupName = (value: string) => value.length >= 1 && value.length <= 90
      && /^[\p{L}\p{Nd}_.()-]+$/u.test(value) && !value.endsWith('.');
    const settingName = (value: string) => value.length >= 1 && value.length <= 260
      && !/[:<>+/&%\\?|\p{Cc}]/u.test(value) && !/[ .]$/.test(value);
    function armParts(value: unknown, target = false): string[] {
      if (typeof value !== 'string' || value.length > 8192 || /[\p{Cc}\\]/u.test(value) || (target && /[?#]/.test(value))) reject();
      const parts = value.split('/');
      if (parts.length < 9 || parts.length % 2 !== 1 || parts[0] !== '' || parts[1]?.toLowerCase() !== 'subscriptions'
        || !uuid.test(parts[2] ?? '') || parts[3]?.toLowerCase() !== 'resourcegroups' || !resourceGroupName(parts[4] ?? '')
        || parts[5]?.toLowerCase() !== 'providers' || !/^[A-Za-z0-9_.-]+$/.test(parts[6] ?? '')
        || parts.slice(7).some((part,index) => part.length === 0 || part === '.' || part === '..'
          || (index % 2 === 0 && !/^[A-Za-z0-9_.-]+$/.test(part)))) reject();
      return parts;
    }
    let url = base; let bytes = 0; let count = 0;
    const seenPages = new Set<string>(); const seenRules = new Set<string>();
    const deadline = performance.now() + limits.timeout;
    for (let page = 0; page < 32; page++) {
      const timeout = Math.floor(deadline-performance.now());
      if (timeout <= 0 || seenPages.has(url)) reject();
      seenPages.add(url);
      const stdout = await run(['rest','--method','get','--url',url,'--subscription',configuration.subscription,'--only-show-errors','--output','json'],{...limits,timeout});
      if (performance.now() > deadline || typeof stdout !== 'string' || Buffer.byteLength(stdout) > limits.maxBuffer) reject();
      bytes += Buffer.byteLength(stdout); if (bytes > 4*1024*1024) reject();
      const body: unknown = JSON.parse(stdout);
      if (!object(body) || !Array.isArray(body.value) || 'error' in body) reject();
      for (const rule of body.value) {
        if (++count > 10_000 || !object(rule) || !object(rule.properties)
          || typeof rule.properties.enabled !== 'boolean') reject();
        armParts(rule.properties.targetResourceUri,true);
        const parts = armParts(rule.id);
        if (parts.length !== 9 || !sameId(parts[2],configuration.subscription)
          || parts[6]?.toLowerCase() !== 'microsoft.insights' || parts[7]?.toLowerCase() !== 'autoscalesettings'
          || !settingName(parts[8] ?? '') || typeof rule.id !== 'string' || seenRules.has(rule.id.toLowerCase())) reject();
        seenRules.add(rule.id.toLowerCase());
        if (rule.properties.enabled && sameId(rule.properties.targetResourceUri,configuration.resourceId)) reject();
      }
      if (body.nextLink === undefined || body.nextLink === null) return;
      url = pageUrl(body.nextLink);
    }
    reject(); // An unfinished list is never evidence that autoscale is absent.
  }
  const scope = (configuration: AzurePoolConfiguration) => ['--resource-group',configuration.resourceGroup,'--name',configuration.scaleSetName];
  async function inventory(configuration: AzurePoolConfiguration): Promise<{capacity:number;instances:AzureInstance[]}> {
    const model = await command(configuration,['vmss','show',...scope(configuration)]);
    if (!object(model) || !sameId(model.id,configuration.resourceId) || !object(model.tags) || model.tags[configuration.ownershipTag.key] !== configuration.ownershipTag.value
      || model.orchestrationMode !== 'Uniform' || model.provisioningState !== 'Succeeded' || model.overprovision !== false
      || !object(model.sku) || !integer(model.sku.capacity) || !object(model.upgradePolicy) || model.upgradePolicy.mode !== 'Manual'
      || !object(model.upgradePolicy.automaticOSUpgradePolicy) || model.upgradePolicy.automaticOSUpgradePolicy.enableAutomaticOSUpgrade !== false
      || !object(model.automaticRepairsPolicy) || model.automaticRepairsPolicy.enabled !== false || !matchingImage(imageReference(model.virtualMachineProfile),configuration.image)) reject();
    // Autoscale settings may live outside the VMSS resource group; inspect the whole subscription.
    await assertNoAutoscale(configuration);
    const rows = await command(configuration,['vmss','list-instances',...scope(configuration)]);
    if (!Array.isArray(rows) || rows.length !== model.sku.capacity) reject();
    const instances = rows.map((row):AzureInstance => {
      if (!object(row) || typeof row.instanceId !== 'string' || !/^(0|[1-9]\d*)$/.test(row.instanceId) || typeof row.vmId !== 'string' || !uuid.test(row.vmId)
        || !sameId(row.id,`${configuration.resourceId}/virtualMachines/${row.instanceId}`) || !['Succeeded','Creating','Updating'].includes(String(row.provisioningState))
        || !matchingImage(imageReference(row),configuration.image)
        || (row.protectionPolicy !== null && row.protectionPolicy !== undefined && (!object(row.protectionPolicy)
          || typeof row.protectionPolicy.protectFromScaleIn !== 'boolean' || typeof row.protectionPolicy.protectFromScaleSetActions !== 'boolean'))) reject();
      return {resourceId:`${configuration.resourceId}/virtualMachines/${row.instanceId}`,instanceId:row.instanceId,vmId:row.vmId.toLowerCase(),workerId:azureWorkerId(configuration.resourceId,row.vmId),
        protected:object(row.protectionPolicy) && row.protectionPolicy.protectFromScaleIn === true && row.protectionPolicy.protectFromScaleSetActions === true,provisioningState:String(row.provisioningState)};
    });
    if (new Set(instances.map(instance=>instance.instanceId)).size !== instances.length || new Set(instances.map(instance=>instance.vmId)).size !== instances.length) reject();
    return {capacity:model.sku.capacity,instances};
  }
  async function native(configuration: AzurePoolConfiguration, instances: AzureInstance[], policy: FleetPolicy): Promise<FleetSnapshot> {
    const observation = await options.observeNative({configuration,instances}); const current = now();
    if (!object(observation) || !integer(observation.observedAt) || observation.observedAt > current || current-observation.observedAt > policy.pollIntervalSeconds*1000
      || !Array.isArray(observation.demands) || !Array.isArray(observation.managedWorkers) || !Array.isArray(observation.permanentWorkers)) reject();
    const demands = observation.demands.map((d): FleetDemand=>{if(!object(d)||!text(d.id)||!text(d.capability))reject();return {id:d.id,capability:d.capability};});
    const permanent = observation.permanentWorkers.map(worker=>nativeWorker(worker,current,policy.pollIntervalSeconds*1000));
    if (permanent.some(worker=>worker.managed || !worker.permanent || instances.some(instance=>instance.workerId===worker.id))) reject();
    const managed = observation.managedWorkers.map(worker=>nativeWorker(worker,current,policy.pollIntervalSeconds*1000));
    if (managed.length !== instances.length || managed.some(worker=>!worker.managed || worker.permanent || !instances.some(instance=>instance.workerId===worker.id))) reject();
    if (new Set([...permanent,...managed].map(worker=>worker.id)).size !== permanent.length+managed.length) reject();
    const attempts = observation.provisioningAttempts === undefined ? [] : observation.provisioningAttempts;
    if (!Array.isArray(attempts) || attempts.some(attempt => !object(attempt) || !exact(attempt,['workerId','vmId','operationId'])
      || !text(attempt.workerId) || typeof attempt.vmId !== 'string' || !uuid.test(attempt.vmId) || !text(attempt.operationId)
      || !instances.some(instance => instance.workerId === attempt.workerId && instance.vmId === attempt.vmId))
      || new Set(attempts.map(attempt => attempt.workerId)).size !== attempts.length) reject();
    const workers = instances.map((instance):FleetWorkerObservation=>{
      const registered=managed.find(worker=>worker.id===instance.workerId);
      if (!registered || (registered.state==='ready' && (!instance.protected || instance.provisioningState!=='Succeeded'))
        || (registered.state==='provisioning' && !attempts.some(attempt => attempt.workerId===instance.workerId && attempt.vmId===instance.vmId))) reject();
      return registered;
    });
    if ([...workers,...permanent].some(worker=>worker.observedAt>observation.observedAt)) reject();
    return {observedAt:observation.observedAt,now:current,demands,workers:[...workers,...permanent],providerAvailable:true};
  }
  function active(operation: FleetMutation): AzurePoolConfiguration {
    if (!operation.policy.enabled || operation.policy.mode!=='active' || !integer(operation.targetCapacity) || operation.targetCapacity>operation.policy.maxWorkers) reject();
    return config(operation.configuration);
  }
  async function guarded(action:()=>Promise<void>): Promise<void> {try {await action();} catch {throw Error('azure_operation_failed');}}
  async function target(operation: FleetMutation, configuration: AzurePoolConfiguration): Promise<AzureInstance> {
    const current = await inventory(configuration); const instance=current.instances.find(item=>item.workerId===operation.workerId);
    const original=operation.snapshot.workers.find(worker=>worker.id===operation.workerId);
    if (!instance || !original || !original.managed || original.permanent || !instance.protected) reject();
    return instance;
  }
  return {
    validateConfiguration(configuration) {
      config(configuration);
      if (configuration.policy.enabled && configuration.policy.mode==='active' && (!options.bootstrap || !options.beginDrain || !options.sealForRemoval || !options.retireRegistration)) throw Error('invalid_azure_configuration');
    },
    async observe(policy, configuration) {
      try { const configured=config(configuration);const current=await inventory(configured);return await native(configured,current.instances,policy); }
      catch {const current=now();return {observedAt:current,now:current,demands:[],workers:[],providerAvailable:false};}
    },
    async provision(operation) {return guarded(async()=>{
      const configuration=active(operation);if(!options.bootstrap)reject();const before=await inventory(configuration);
      if (operation.type!=='provision' || operation.targetCapacity<=before.capacity || operation.targetCapacity!==before.capacity+operation.count || !integer(operation.count) || operation.count<1
        || before.instances.length!==operation.snapshot.workers.filter(worker=>worker.managed&&!worker.permanent).length
        || before.instances.some(instance=>!operation.snapshot.workers.some(worker=>worker.id===instance.workerId&&worker.managed&&!worker.permanent))) reject();
      await native(configuration,before.instances,operation.policy);
      await command(configuration,['vmss','scale',...scope(configuration),'--new-capacity',String(operation.targetCapacity)]);
      const after=await inventory(configuration);if(after.capacity!==operation.targetCapacity || before.instances.some(instance=>!after.instances.some(next=>next.workerId===instance.workerId)))reject();
      const added=after.instances.filter(instance=>!before.instances.some(old=>old.workerId===instance.workerId));if(added.length!==operation.count)reject();
      for(const instance of added) await command(configuration,['vmss','update',...scope(configuration),'--instance-id',instance.instanceId,'--protect-from-scale-in','true','--protect-from-scale-set-actions','true']);
      const protectedInventory=await inventory(configuration);
      for(const instance of added) {
        const fresh=protectedInventory.instances.find(item=>item.workerId===instance.workerId);if(!fresh?.protected || fresh.provisioningState!=='Succeeded')reject();
        await options.bootstrap({configuration,operation,instance:fresh});
      }
      const complete=await inventory(configuration);const observed=await native(configuration,complete.instances,operation.policy);
      if(complete.capacity!==operation.targetCapacity || added.some(instance=>!observed.workers.some(worker=>worker.id===instance.workerId&&worker.state==='ready')))reject();
    });},
    async beginDrain(operation) {return guarded(async()=>{
      const configuration=active(operation);if(operation.type!=='beginDrain'||!options.beginDrain)reject();const instance=await target(operation,configuration);
      await options.beginDrain({configuration,operation,instance});
    });},
    async remove(operation) {return guarded(async()=>{
      const configuration=active(operation);if(operation.type!=='remove'||!options.sealForRemoval||!options.retireRegistration)reject();
      const instance=await target(operation,configuration);const started=now();const seal=await options.sealForRemoval({configuration,operation,instance});const checked=now();
      if(!object(seal)||seal.workerId!==instance.workerId||seal.vmId!==instance.vmId||seal.operationId!==operation.operationId||seal.beginDrainConfirmed!==true||seal.fenced!==true
        ||!text(seal.drainGeneration)||seal.active!==0||seal.claims!==0||seal.processes!==0||seal.pendingTransfers!==false||seal.pendingActivationIntents!==false
        ||seal.coordinatorCheckpointAcknowledged!==true||seal.clean!==true||seal.pendingHumanReturns!==false||!integer(seal.observedAt)||seal.observedAt<started||seal.observedAt>checked
        ||!integer(seal.drainConfirmedAt)||seal.observedAt<=seal.drainConfirmedAt)reject();
      const fresh=await target(operation,configuration);if(fresh.vmId!==instance.vmId||fresh.instanceId!==instance.instanceId)reject();
      await command(configuration,['vmss','delete-instances',...scope(configuration),'--instance-ids',instance.instanceId],'bodyless-delete');
      const after=await inventory(configuration);if(after.instances.some(item=>item.instanceId===instance.instanceId||item.vmId===instance.vmId))reject();
      await options.retireRegistration({configuration,operation,instance});
    });},
    async inspectOperation(operation, configuration) {
      try {
        const configured=config(configuration);const current=await inventory(configured);
        if(operation.type==='provision') {
          if(current.capacity!==operation.targetCapacity)return 'pending';const observed=await native(configured,current.instances,configuration.policy);
          const old=new Set(operation.snapshot.workers.filter(worker=>worker.managed&&!worker.permanent).map(worker=>worker.id));
          const added=current.instances.filter(instance=>!old.has(instance.workerId));
          if(added.length!==operation.count||[...old].some(id=>!current.instances.some(instance=>instance.workerId===id)))return 'unknown';
          return added.every(instance=>observed.workers.some(worker=>worker.id===instance.workerId&&worker.state==='ready'))?'succeeded':'pending';
        }
        if(operation.type==='remove'&&current.instances.some(instance=>instance.workerId===operation.workerId))return 'pending';
        // Deletion absence alone cannot attest credential retirement or distinguish external loss.
        return options.inspectNative ? await options.inspectNative({configuration:configured,instances:current.instances,operation}) : 'unknown';
      } catch {return 'unknown';}
    },
  };
}
