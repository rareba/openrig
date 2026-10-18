import { existsSync, lstatSync, readFileSync, readdirSync, readlinkSync, realpathSync, unlinkSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { procIdentity, type LinuxWorkerManifest } from './linux-privileged-worker-boundary.js';

/** Evidence for the single default tmux listener; never a general special-file exemption. */
export interface ManagedTmuxIpc {
  kind: 'tmux-unix-listener'; path: string; uid: number; device: string; inode: string;
  mode: number; kernelInode: string; pid: number; startTime: string; observedAt: number;
}
function fail(): never { throw Error('managed_worker_ipc_unproven'); }
export function managedTmuxPath(m: LinuxWorkerManifest): string {
  return join(m.linux.temp, `tmux-${m.uid}`, 'default');
}
function endpointRows(): string[][] {
  return readFileSync('/proc/net/unix', 'utf8').trim().split('\n').slice(1).map(line => line.trim().split(/\s+/));
}
export function sameManagedSocket(proof: ManagedTmuxIpc): boolean {
  const st = lstatSync(proof.path, {bigint:true});
  return proof.kind === 'tmux-unix-listener' && realpathSync(proof.path) === proof.path && st.isSocket()
    && st.uid === BigInt(proof.uid) && st.nlink === 1n && st.dev.toString() === proof.device && st.ino.toString() === proof.inode && Number(st.mode & 0o777n) === proof.mode;
}
function parent(m: LinuxWorkerManifest): void {
  const path = dirname(managedTmuxPath(m));
  const st = lstatSync(path);
  if (realpathSync(path) !== path || !st.isDirectory() || st.uid !== m.uid || (st.mode & 0o777) !== 0o700) fail();
}
export function inspectManagedTmuxIpc(m: LinuxWorkerManifest): ManagedTmuxIpc[] {
  const path = managedTmuxPath(m);
  if (!existsSync(path)) return [];
  parent(m);
  const st = lstatSync(path, {bigint:true});
  if (!st.isSocket() || st.uid !== BigInt(m.uid) || (st.mode & 0o077n) !== 0n || st.nlink !== 1n) fail();
  const rows = endpointRows().filter(row => row[7] === path);
  if (rows.length !== 1) fail();
  const row = rows[0]!;
  if (row.length !== 8 || row[3] !== '00010000' || row[4] !== '0001' || row[5] !== '01' || !/^[1-9][0-9]*$/.test(row[6]!)) fail();
  const holders = [];
  for (const name of readdirSync('/proc')) {
    if (!/^\d+$/.test(name)) continue;
    const status = readFileSync(`/proc/${name}/status`, 'utf8');
    if (Number(/^Uid:\s+(\d+)/m.exec(status)?.[1]) !== m.uid) continue;
    const process = procIdentity(Number(name));
    if (process.executable !== m.linux.tmux) continue;
    const fds = readdirSync(`/proc/${name}/fd`);
    if (!fds.some(fd => readlinkSync(`/proc/${name}/fd/${fd}`) === `socket:[${row[6]}]`)) continue;
    const env = readFileSync(`/proc/${name}/environ`, 'utf8').split('\0').filter(v => v.startsWith('TMUX_TMPDIR='));
    if (process.cgroup !== `/system.slice/${m.unit}` || env.length !== 1 || env[0] !== `TMUX_TMPDIR=${m.linux.temp}`) fail();
    if (JSON.stringify(procIdentity(process.pid)) !== JSON.stringify(process)) fail();
    holders.push(process);
  }
  if (holders.length !== 1) fail();
  const holder = holders[0]!;
  const proof: ManagedTmuxIpc = {kind:'tmux-unix-listener',path,uid:m.uid,device:st.dev.toString(),inode:st.ino.toString(),mode:Number(st.mode&0o777n),kernelInode:row[6]!,pid:holder.pid,startTime:holder.startTime,observedAt:Date.now()};
  if (!sameManagedSocket(proof)) fail();
  return [proof];
}
/** Caller must independently prove the sealed whole unit and every dedicated UID process absent first. */
export function retireStoppedTmuxIpc(m: LinuxWorkerManifest, proofs: ManagedTmuxIpc[]): Array<{ evidence: ManagedTmuxIpc; disposition: 'already-absent' | 'retired-dead-socket' }> {
  if (proofs.length > 1) fail();
  const path = managedTmuxPath(m);
  const rows = endpointRows();
  if (rows.some(row => row[7] === path || proofs.some(p => row[6] === p.kernelInode))) fail();
  if (!proofs.length) {
    if (existsSync(path)) fail();
    return [];
  }
  const proof = proofs[0]!;
  if (proof.kind !== 'tmux-unix-listener' || proof.path !== path || proof.uid !== m.uid) fail();
  if (!existsSync(path)) return [{evidence:proof,disposition:'already-absent'}];
  parent(m);
  if (!sameManagedSocket(proof)) fail();
  unlinkSync(path);
  if (existsSync(path) || endpointRows().some(row => row[7] === path || row[6] === proof.kernelInode)) fail();
  return [{evidence:proof,disposition:'retired-dead-socket'}];
}
