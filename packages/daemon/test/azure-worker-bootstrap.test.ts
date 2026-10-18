import { afterEach, describe, expect, it } from 'vitest';
import { generateKeyPairSync, createPublicKey, randomBytes } from 'node:crypto';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, realpathSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import Database from 'better-sqlite3';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { migrate } from '../src/db/migrate.js';
import { ElasticWorkerControl } from '../src/domain/elastic-worker-control.js';
import { openEnrollment, openWorkerMaterial, sealEnrollment, sealWorkerMaterial, type EnrollmentBinding } from '../src/domain/elastic-enrollment.js';
import { azureWorkerId } from '../src/adapters/azure-elastic-fleet.js';
import { AzureWorkerBootstrap, type WorkerBootstrapManifest, type PrivilegedWorkerBoundary, assertManagedCommandSuccess, readWorkerRuntimeMaterial } from '../src/adapters/azure-worker-bootstrap.js';
const controller=generateKeyPairSync('rsa',{modulusLength:3072});
const cleanup:Array<()=>void>=[];afterEach(()=>cleanup.splice(0).forEach(fn=>fn()));
function fixture() {
 const root=realpathSync(mkdtempSync(join(tmpdir(),'elastic-bootstrap-')));const home=join(root,'business');mkdirSync(home);writeFileSync(join(home,'report.txt'),'saved');
 const db=new Database(':memory:');migrate(db,ALL_MIGRATIONS);cleanup.push(()=>{db.close();rmSync(root,{recursive:true,force:true});});
 const binding:EnrollmentBinding={version:1,resourceId:'/subscriptions/a/resourceGroups/b/providers/Microsoft.Compute/virtualMachineScaleSets/c',vmId:'a0000000-0000-0000-0000-000000000001',nonce:randomBytes(32).toString('base64'),manifestDigest:'a'.repeat(64),operationId:'enrollment-a',fleetEpoch:'epoch-a'};
 const manifest:WorkerBootstrapManifest={version:1,resourceId:binding.resourceId,workerUser:'openrig-elastic',unit:'openrig-elastic.service',uid:2100,gid:2100,controlDirectory:join(root,'control'),ownedRoots:[home],pins:[],materialFiles:{piAuthFile:join(root,'auth.json'),piSettingsFile:join(root,'settings.json'),piModelsFile:join(root,'models.json')},bindPolicy:'private-ipv4',port:7433};
 const calls:string[]=[];let processes=1;const os:PrivilegedWorkerBoundary={assertTrustedEnvironment:()=>{calls.push('verify');},configureWorker:()=>{calls.push('configure');},startUnit:()=>{calls.push('start');},installSealMarker:()=>{calls.push('marker');},stopUnit:()=>{calls.push('stop');processes=0;},processes:()=>({observedAt:Date.now(),complete:true,idle:true,processes,uid:2100,cgroup:'/system.slice/openrig-elastic.service'})};
 const helper=new AzureWorkerBootstrap(manifest,binding.manifestDigest,new ElasticWorkerControl(db),os);
 return{root,home,db,binding,manifest,helper,os,calls};
}
describe('trusted worker bootstrap protocol',()=>{
 it('requires actual terminal Run Command script success and exit zero',()=>{expect(()=>assertManagedCommandSuccess({provisioningState:'Succeeded',output:'ok'})).toThrow('worker_bootstrap_rejected');expect(()=>assertManagedCommandSuccess({executionState:'Succeeded',exitCode:1,output:'ok'})).toThrow();expect(assertManagedCommandSuccess({executionState:'Succeeded',exitCode:0,output:'ok'})).toBe('ok');});
 it('prepares once and returns only an encrypted idempotent worker bearer',()=>{const f=fixture();const publicPem=controller.publicKey.export({type:'spki',format:'pem'}).toString();const prepared=f.helper.prepare(f.binding,publicPem);expect(f.helper.prepare(f.binding,publicPem)).toEqual(prepared);const material=sealWorkerMaterial(Buffer.from(JSON.stringify({version:1,piAuth:{test:'secret'},piModels:{providers:{test:{baseUrl:'https://example.invalid'}}},piSettings:{model:'pinned'}})),f.binding,createPublicKey(prepared.publicKeyPem));const envelope=f.helper.enroll(f.binding,material);const bearer=openEnrollment(envelope,f.binding,controller.privateKey);expect(bearer.length).toBeGreaterThan(32);expect(envelope).not.toContain(bearer.toString());expect(openEnrollment(f.helper.enroll(f.binding,material),f.binding,controller.privateKey)).toEqual(bearer);expect(f.calls.filter(x=>x==='configure')).toHaveLength(1);expect(()=>f.helper.prepare({...f.binding,vmId:'b0000000-0000-0000-0000-000000000001'},publicPem)).toThrow();});
 it('separates encrypted material from 4096-byte bounded credential output',()=>{const f=fixture();const payload=Buffer.alloc(3000,42);const envelope=sealWorkerMaterial(payload,f.binding,controller.publicKey);expect(openWorkerMaterial(envelope,f.binding,controller.privateKey)).toEqual(payload);expect(()=>openEnrollment(envelope,f.binding,controller.privateKey)).toThrow();expect(()=>openWorkerMaterial(sealEnrollment(Buffer.from('token'),f.binding,controller.publicKey),f.binding,controller.privateKey)).toThrow();expect(()=>sealEnrollment(payload,f.binding,controller.publicKey)).toThrow();});
 it('requires exact checkpoint, seals admission before stop, and detects post-stop change',()=>{const f=fixture();const identity={workerId:azureWorkerId(f.manifest.resourceId,f.binding.vmId),vmId:f.binding.vmId,manifestDigest:f.binding.manifestDigest};f.helper.control.enroll(identity);f.helper.beginDrain(identity,'drain-a');const checkpoint=f.helper.checkpoint(identity,1);f.helper.acknowledgeCheckpoint(identity,1,checkpoint);const stop=f.os.stopUnit;f.os.stopUnit=()=>{expect(f.helper.control.read()?.phase).toBe('sealed');stop();writeFileSync(join(f.home,'late.txt'),'unsaved');};expect(()=>f.helper.seal(identity,1,'remove-a')).toThrow('worker_bootstrap_rejected');expect(f.calls).toContain('marker');expect(f.helper.control.read()?.phase).toBe('sealed');});
 it('returns removal proof only after empty cgroup and UID plus stable ledgers and business hashes',()=>{const f=fixture();const identity={workerId:azureWorkerId(f.manifest.resourceId,f.binding.vmId),vmId:f.binding.vmId,manifestDigest:f.binding.manifestDigest};f.helper.control.enroll(identity);f.helper.beginDrain(identity,'drain-a');f.helper.acknowledgeCheckpoint(identity,1,f.helper.checkpoint(identity,1));const receipt=f.helper.seal(identity,1,'remove-a');expect(receipt).toMatchObject({workerId:azureWorkerId(f.manifest.resourceId,f.binding.vmId),active:0,processes:0,clean:true,coordinatorCheckpointAcknowledged:true,fenced:true});expect(f.calls.indexOf('marker')).toBeLessThan(f.calls.indexOf('stop'));});
 it('refuses stop when custody, stale process proof, wrong UID or missing checkpoint persists',()=>{const f=fixture();const identity={workerId:azureWorkerId(f.manifest.resourceId,f.binding.vmId),vmId:f.binding.vmId,manifestDigest:f.binding.manifestDigest};f.helper.control.enroll(identity);f.helper.beginDrain(identity,'drain-a');expect(()=>f.helper.seal(identity,1,'remove-a')).toThrow();expect(f.calls).not.toContain('stop');f.helper.acknowledgeCheckpoint(identity,1,f.helper.checkpoint(identity,1));f.os.processes=()=>({observedAt:0,complete:true,idle:true,processes:0,uid:2100,cgroup:'/system.slice/openrig-elastic.service'});expect(()=>f.helper.seal(identity,1,'remove-a')).toThrow();expect(f.calls).not.toContain('stop');});
 it('retains the worker on a surviving UID process or post-stop SQLite outbox write',()=>{
  for (const mode of ['survivor','outbox']) {
   const f=fixture();const identity={workerId:azureWorkerId(f.manifest.resourceId,f.binding.vmId),vmId:f.binding.vmId,manifestDigest:f.binding.manifestDigest};f.helper.control.enroll(identity);f.helper.beginDrain(identity,'drain-a');f.helper.acknowledgeCheckpoint(identity,1,f.helper.checkpoint(identity,1));
   const stop=f.os.stopUnit;f.os.stopUnit=()=>{if(mode==='outbox'){stop();f.db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES ('late','a','b','{}','','pending')").run();}};
   expect(()=>f.helper.seal(identity,1,'remove-a')).toThrow('worker_bootstrap_rejected');
  }
 });

 it('reads separate explicit Pi auth, settings and custom-provider models files without remapping them',()=>{
  const f=fixture();writeFileSync(f.manifest.materialFiles.piAuthFile,JSON.stringify({auth:'secret'}));writeFileSync(f.manifest.materialFiles.piSettingsFile,JSON.stringify({model:'test'}));writeFileSync(f.manifest.materialFiles.piModelsFile,JSON.stringify({providers:{test:{apiKey:'private-provider-key'}}}));
  expect(JSON.parse(readWorkerRuntimeMaterial(f.manifest.materialFiles).toString())).toEqual({version:1,piAuth:{auth:'secret'},piSettings:{model:'test'},piModels:{providers:{test:{apiKey:'private-provider-key'}}}});
  writeFileSync(f.manifest.materialFiles.piModelsFile,'x'.repeat(64*1024));expect(()=>readWorkerRuntimeMaterial(f.manifest.materialFiles)).toThrow('worker_bootstrap_rejected');
 });

 it.each(['draining','sealed'])('rejects uncertain initial enrollment retries before side effects after native state becomes %s',(phase)=>{
  const f=fixture();const prepared=f.helper.prepare(f.binding,controller.publicKey.export({type:'spki',format:'pem'}).toString());
  const material=sealWorkerMaterial(Buffer.from(JSON.stringify({version:1,piAuth:{},piSettings:{},piModels:{}})),f.binding,createPublicKey(prepared.publicKeyPem));
  const phases:string[]=[];let fail=true;f.os.startUnit=()=>{phases.push(f.helper.control.read()!.phase);if(fail)throw Error('uncertain-start');};
  expect(()=>f.helper.enroll(f.binding,material)).toThrow('worker_bootstrap_rejected');
  const identity=f.helper.control.read()!;f.helper.beginDrain(identity,'drain-a');
  if(phase==='sealed'){f.helper.acknowledgeCheckpoint(identity,1,f.helper.checkpoint(identity,1));expect(f.helper.seal(identity,1,'remove-a').clean).toBe(true);}
  const configured=f.calls.filter(call=>call==='configure').length;fail=false;
  expect(()=>f.helper.enroll(f.binding,material)).toThrow('worker_bootstrap_rejected');expect(phases).toEqual(['ready']);expect(f.calls.filter(call=>call==='configure')).toHaveLength(configured);expect(f.helper.control.read()?.phase).toBe(phase);
 });
 it('resumes an uncertain enrollment with the same bearer while native phase remains ready',()=>{
  const f=fixture();const prepared=f.helper.prepare(f.binding,controller.publicKey.export({type:'spki',format:'pem'}).toString());const material=sealWorkerMaterial(Buffer.from(JSON.stringify({version:1,piAuth:{},piSettings:{},piModels:{}})),f.binding,createPublicKey(prepared.publicKeyPem));
  const bearer=JSON.parse(readFileSync(join(f.manifest.controlDirectory,'enrollment.json'),'utf8')).bearer;
  f.os.startUnit=()=>{throw Error('uncertain-start');};expect(()=>f.helper.enroll(f.binding,material)).toThrow('worker_bootstrap_rejected');f.os.startUnit=()=>{};
  expect(openEnrollment(f.helper.enroll(f.binding,material),f.binding,controller.privateKey).toString()).toBe(bearer);
 });

});
