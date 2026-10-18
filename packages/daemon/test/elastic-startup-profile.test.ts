import {afterEach,expect,it,vi} from 'vitest';
const boot=vi.hoisted(()=>vi.fn(()=>{throw Error('default kernel must not start');}));
vi.mock('../src/domain/kernel-boot.js',()=>({bootKernelIfNeeded:boot}));
vi.mock('../src/domain/elastic-worker-guard.js',async original=>({...await original<object>(),assertElasticEffectAllowed:vi.fn()}));
vi.mock('../src/adapters/elastic-worker-foreground.js',async original=>({...await original<object>(),loadForegroundManifest:vi.fn(()=>({linux:{piVersion:'1.0.4'}}))}));
import {createDaemon} from '../src/startup.js';
afterEach(()=>{delete process.env.OPENRIG_ELASTIC_GUARD;});
it('composes an opted-in finite Pi worker without default kernel rows or unrelated runtime probes',async()=>{process.env.OPENRIG_ELASTIC_GUARD='/verified-by-boundary-fixture';const commands:string[]=[];const daemon=await createDaemon({dbPath:':memory:',cmuxFactory:async()=>{throw Object.assign(Error('absent'),{code:'ENOENT'});},tmuxExec:async command=>{commands.push(command);return '';}});try{expect(boot).not.toHaveBeenCalled();expect(daemon.db.prepare('SELECT * FROM nodes').all()).toEqual([]);expect(commands.some(command=>/codex|claude|omp/.test(command))).toBe(false);expect(daemon.deps.kernelBootTracker).toBeUndefined();}finally{daemon.eventLoopMonitor.stop();daemon.db.close();}},30000);
