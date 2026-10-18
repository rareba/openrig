import { afterEach, expect, it, vi } from 'vitest';
import { createServer, type Server } from 'node:net';
import { mkdtempSync, mkdirSync, realpathSync, lstatSync, writeFileSync, readFileSync, rmSync, chmodSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { inspectWorkerFilesystem } from '../src/adapters/elastic-worker-probe.js';
import { captureFinalCheckpoint, decodeCheckpointArchive } from '../src/adapters/elastic-worker-checkpoint.js';
import * as boundary from '../src/adapters/linux-privileged-worker-boundary.js';
import { inspectManagedTmuxIpc, retireStoppedTmuxIpc } from '../src/adapters/managed-tmux-ipc.js';
const proc = vi.hoisted(() => ({path:'',uid:0,temp:'',endpoint:true,fd:'socket:[500]',env:'',exe:'/usr/bin/tmux',cgroup:'/system.slice/openrig-elastic.service'}));
vi.mock('node:fs',async original => {
  const fs=await original<typeof import('node:fs')>();
  return {...fs,
    readFileSync:(path:any,...args:any[]) => {
      if(path==='/proc/net/unix') return 'Num RefCount Protocol Flags Type St Inode Path\n'+(proc.endpoint?`000: 00000002 00000000 00010000 0001 01 500 ${proc.path}\n`:'');
      if(path==='/proc/123/status') return `Uid:\t${proc.uid}\t${proc.uid}\t${proc.uid}\t${proc.uid}\n`;
      if(path==='/proc/123/environ') return proc.env;
      return (fs.readFileSync as any)(path,...args);
    },
    readdirSync:(path:any,...args:any[]) => path==='/proc'?['123']:path==='/proc/123/fd'?['6']:(fs.readdirSync as any)(path,...args),
    readlinkSync:(path:any,...args:any[]) => path==='/proc/123/fd/6'?proc.fd:(fs.readlinkSync as any)(path,...args),
  };
});
const roots: string[] = [];
const servers: Server[] = [];
afterEach(async () => { for (const server of servers.splice(0)) if(server.listening) await new Promise<void>(r => server.close(() => r())); for (const root of roots.splice(0)) rmSync(root,{recursive:true,force:true}); vi.restoreAllMocks(); });
async function fixture() {
  const root=realpathSync(mkdtempSync(join(realpathSync('/tmp'),'ipc-'))); roots.push(root);
  const dir=join(root,`tmux-${process.getuid!()}`); mkdirSync(dir,{mode:0o700});
  const path=join(dir,'default'); const server=createServer(); servers.push(server);
  await new Promise<void>(resolve=>server.listen(path,resolve));
  chmodSync(path,0o700);
  const stat=lstatSync(path);
  const proof={kind:'tmux-unix-listener' as const,path,uid:stat.uid,device:stat.dev.toString(),inode:stat.ino.toString(),mode:stat.mode&0o777,kernelInode:'500',pid:123,startTime:'42',observedAt:Date.now()};
  writeFileSync(join(root,'ordinary-temp-work'),'must survive');
  Object.assign(proc,{path,uid:stat.uid,temp:root,endpoint:true,fd:'socket:[500]',env:`TMUX_TMPDIR=${root}\0`,exe:'/usr/bin/tmux',cgroup:'/system.slice/openrig-elastic.service'});
  vi.spyOn(boundary,'procIdentity').mockImplementation(() => ({pid:123,startTime:'42',uid:proc.uid,ppid:1,state:'S',cgroup:proc.cgroup,executable:proc.exe,argv:['tmux']}));
  const manifest={uid:stat.uid,unit:'openrig-elastic.service',linux:{temp:root,tmux:'/usr/bin/tmux'}} as boundary.LinuxWorkerManifest;
  return {root,path,proof,server,manifest};
}
it('inventories an exact verified managed socket while retaining ordinary temporary work',async()=>{
  const f=await fixture();
  const proof=inspectWorkerFilesystem([f.root],[],[],[f.proof]);
  expect(proof.complete).toBe(true);
  expect(proof.files.map(x=>x.path)).toContain(join(f.root,'ordinary-temp-work'));
  expect(proof.managedIpc).toEqual([f.proof]);
});
it('retains unproven, foreign, replaced and stale socket artifacts',async()=>{
  const f=await fixture();
  expect(inspectWorkerFilesystem([f.root],[]).complete).toBe(false);
  for(const change of [{uid:f.proof.uid+1},{inode:(BigInt(f.proof.inode)+1n).toString()},{observedAt:Date.now()-10000},{path:f.path+'-other'}])
    expect(inspectWorkerFilesystem([f.root],[],[],[{...f.proof,...change}]).complete).toBe(false);
});

it('corroborates only the exact pinned tmux listener, fd, namespace path and dedicated UID',async()=>{
  const f=await fixture();
  expect(inspectManagedTmuxIpc(f.manifest)).toEqual([expect.objectContaining({...f.proof,observedAt:expect.any(Number)})]);
  for(const change of [{fd:'socket:[999]'},{exe:'/usr/bin/foreign'},{env:'TMUX_TMPDIR=/tmp\0'},{cgroup:'/escaped'},{path:'/tmp/tmux-0/default'}]) {
    const saved={...proc};Object.assign(proc,change);
    expect(()=>inspectManagedTmuxIpc(f.manifest)).toThrow();Object.assign(proc,saved);
  }
});
it('retains a live or replaced socket, and retires only its exact dead inode with audit evidence',async()=>{
  const f=await fixture();
  expect(()=>retireStoppedTmuxIpc(f.manifest,[f.proof])).toThrow();
  proc.endpoint=false;
  expect(()=>retireStoppedTmuxIpc(f.manifest,[{...f.proof,inode:(BigInt(f.proof.inode)+1n).toString()}])).toThrow();
  expect(existsSync(f.path)).toBe(true);
  const audit=retireStoppedTmuxIpc(f.manifest,[f.proof]);
  expect(audit).toEqual([{evidence:f.proof,disposition:'retired-dead-socket'}]);
  expect(existsSync(f.path)).toBe(false);
  expect(existsSync(join(f.root,'ordinary-temp-work'))).toBe(true);
  const control=realpathSync(mkdtempSync(join(realpathSync('/tmp'),'ipc-control-')));roots.push(control);
  const checkpoint=await captureFinalCheckpoint({binding:{workerId:'worker',vmId:'vm',manifestDigest:'a'.repeat(64),fleetEpoch:'epoch',drainGeneration:1,removalOperationId:'remove'},roots:[f.root],databases:[],controlDirectory:control,journal:Buffer.alloc(0),audit:{managedIpc:audit},gitRefs:[]});
  const archive=decodeCheckpointArchive(readFileSync(checkpoint.archivePath));
  expect(archive.files.find(x=>x.path==='roots/0/ordinary-temp-work')?.bytes.toString()).toBe('must survive');
  expect(JSON.parse(archive.files.find(x=>x.path==='audit/control.json')!.bytes.toString()).managedIpc).toEqual(audit);
  expect(retireStoppedTmuxIpc(f.manifest,[f.proof])).toEqual([{evidence:f.proof,disposition:'already-absent'}]);
});
