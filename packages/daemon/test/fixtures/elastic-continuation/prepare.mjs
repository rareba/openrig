/** Run only in a disposable root container, with a task-owned volume at --root.
 * node packages/daemon/test/fixtures/elastic-continuation/prepare.mjs --root /elastic-continuation-fixture --work /tmp/elastic-continuation-fixture-work --uid 501 --gid 501
 * The following test worker mounts --root read-only and exports OPENRIG_CONTINUATION_TEST_ROOT=--root.
 * No production path accepts this fixture as root issuance or systemd/cgroup proof.
 */
import { build } from 'esbuild';
import { mkdirSync,writeFileSync,readFileSync,realpathSync,copyFileSync,symlinkSync,existsSync,readdirSync,lstatSync,chmodSync } from 'node:fs';
import { dirname,join,resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
const arg=name=>process.argv[process.argv.indexOf(name)+1];
const root=arg('--root'),work=arg('--work'),uid=Number(arg('--uid')),gid=Number(arg('--gid'));
if(process.platform!=='linux'||process.getuid()!==0||root!=='/elastic-continuation-fixture'||work!=='/tmp/elastic-continuation-fixture-work'||uid!==501||gid!==501)throw Error('fixture_arguments_rejected');
process.umask(0o022);
if(existsSync(join(root,'manifest.json')))throw Error('fixture_already_prepared');
const here=dirname(fileURLToPath(import.meta.url)),repo=resolve(here,'../../../../..');
const source=join(root,'runtime'),piRoot=join(root,'pi');
for(const p of [root,source,piRoot,join(root,'control')]){mkdirSync(p,{recursive:true,mode:0o755});chmodSync(p,0o755);}
writeFileSync(join(root,'package.json'),JSON.stringify({type:'module'}));
symlinkSync(join(repo,'node_modules'),join(root,'node_modules'));
await build({entryPoints:[join(here,'runner.ts')],outfile:join(source,'pi-runner.js'),bundle:true,platform:'node',format:'esm',packages:'external',alias:{'@openrig/shared':join(repo,'packages/shared/src/index.ts')},define:{'import.meta.url':JSON.stringify('file:///synthetic-imported-runner.js')},sourcemap:false,logLevel:'warning'});
copyFileSync(join(here,'rpc.mjs'),join(piRoot,'rpc.js'));
for(const name of ['helper.js','daemon.js','probe.js'])writeFileSync(join(source,name),'throw Error("synthetic_fixture_not_a_service");\n');
writeFileSync(join(root,'ca.pem'),'SYNTHETIC-NO-NETWORK\n');
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const tree=path=>{const rows=[];const walk=p=>{const st=lstatSync(p);if(st.isDirectory()){rows.push({path:p.slice(path.length),mode:st.mode&0o777,type:'directory'});for(const name of readdirSync(p).sort())walk(join(p,name));}else rows.push({path:p.slice(path.length),mode:st.mode&0o777,sha256:hash(readFileSync(p))});};walk(path);return hash(JSON.stringify(rows));};
const node=realpathSync(process.execPath),tmux=realpathSync(execFileSync('/bin/sh',['-c','command -v tmux'],{encoding:'utf8'}).trim()),shell=realpathSync('/bin/sh');
const home=join(work,'home'),database=join(home,'openrig.db');
const manifest={version:1,resourceId:'/subscriptions/fixture/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture',workerUser:'openrig-elastic',unit:'openrig-elastic.service',uid,gid,controlDirectory:join(root,'control'),ownedRoots:[work],bindPolicy:'private-ipv4',port:7433,
 pins:[{path:source,kind:'tree',sha256:tree(source)},{path:piRoot,kind:'tree',sha256:tree(piRoot)},...Array.from(new Set([node,tmux,shell,join(root,'ca.pem')])).map(path=>({path,kind:'file',sha256:hash(readFileSync(path))}))],
 materialFiles:{piAuthFile:join(root,'unused-auth.json'),piSettingsFile:join(root,'unused-settings.json'),piModelsFile:join(root,'unused-models.json')},
 linux:{imageReference:'synthetic-source-fixture-v1',sourceRoot:source,piRoot,node,daemon:join(source,'daemon.js'),helper:join(source,'helper.js'),probe:join(source,'probe.js'),pi:join(piRoot,'rpc.js'),tmux,shell,home,database,databases:[database],stateRoot:join(home,'state/pi'),temp:join(work,'tmp'),guard:join(root,'continuation.json'),environment:join(root,'unused.env'),identity:join(root,'unused-identity.json'),seats:['owner@rig'],piVersion:'1.0.4',upload:{origin:'https://10.0.0.1:8443',privateIPv4:'10.0.0.1',caFile:join(root,'ca.pem'),routePrefix:'/api/elastic-checkpoints/',maxBytes:1024}}};
const bytes=JSON.stringify(manifest);writeFileSync(join(root,'manifest.json'),bytes,{mode:0o644});
const identity={workerId:'worker',vmId:'11111111-1111-4111-8111-111111111111',manifestDigest:hash(bytes)};
for(const [file,extra]of Object.entries({continuation:{phase:'draining',generation:2,purpose:'continuation',operationId:'22222222-2222-4222-8222-222222222222'},old:{phase:'draining',generation:1,purpose:'removal',operationId:'begin-1'},transition:{phase:'transition',generation:2,purpose:'transition',operationId:'22222222-2222-4222-8222-222222222222'},sealed:{phase:'sealed',generation:2,purpose:'continuation',operationId:'22222222-2222-4222-8222-222222222222'}}))writeFileSync(join(root,file+'.json'),JSON.stringify({version:1,...identity,...extra}),{mode:0o644});
writeFileSync(join(root,'fixture.json'),JSON.stringify({work,uid,gid,identity,runner:join(source,'pi-runner.js'),manifestDigest:identity.manifestDigest}),{mode:0o644});
console.log(JSON.stringify({root,work,uid,gid,manifestDigest:identity.manifestDigest,tmux}));
