import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { existsSync, lstatSync, readdirSync, readFileSync, realpathSync } from 'node:fs';
import { isAbsolute, join, resolve, sep } from 'node:path';
import type { ElasticWorkerControl, ElasticWorkerLedger, ElasticWorkerState } from '../domain/elastic-worker-control.js';
import { workerDigest } from '../domain/elastic-worker-control.js';

export interface WorkerFileProof { path: string; bytes: number; sha256: string }
export interface WorkerRefProof { repo: string; ref: string; sha: string }
export interface WorkerFilesystemProof { complete: boolean; blockers: string[]; files: WorkerFileProof[]; refs: WorkerRefProof[]; digest: string }
export interface WorkerCheckpoint { digest: string; files: WorkerFileProof[]; refs: WorkerRefProof[] }
export interface WorkerProcessProof { observedAt: number; complete: boolean; idle: boolean; processes: number; uid: number; cgroup: string }
export interface ElasticWorkerProbe {
  version: 1; observedAt: number; state: ElasticWorkerState; ledger: ElasticWorkerLedger;
  filesystem: WorkerFilesystemProof; process: WorkerProcessProof; checkpointAcknowledged: boolean; drainEligible?: boolean | null; clean: boolean;
}
function inside(path: string, root: string): boolean { return path === root || path.startsWith(root + sep); }
function git(cwd: string, args: string[]): string {
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('GIT_')));
  return execFileSync('/usr/bin/git', ['--no-optional-locks', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null', '-c', `safe.directory=${cwd}`, '-c', 'protocol.allow=never',
    '-c', 'core.sshCommand=/usr/bin/false', '-c', 'core.gitProxy=/usr/bin/false', '-c', 'credential.helper=', ...args], { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 10_000, maxBuffer: 8 * 1024 * 1024,
    env: { ...inherited, GIT_TERMINAL_PROMPT: '0', GIT_NO_LAZY_FETCH: '1', GIT_ALLOW_PROTOCOL: '', GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null' } });
}
/** Owned roots are a trusted enrollment inventory, not worker-provided deletion exemptions. */
export function inspectWorkerFilesystem(ownedRoots: readonly string[], declaredCwds: readonly string[], excludedSqlitePaths: readonly string[] = []): WorkerFilesystemProof {
  const blockers = new Set<string>(); const files: WorkerFileProof[] = []; const refs: WorkerRefProof[] = [];
  const roots: string[] = []; const repositories = new Set<string>(); const visited = new Set<string>(); let bytes = 0;
  try {
    if (!ownedRoots.length || ownedRoots.length > 128) throw Error();
    for (const root of ownedRoots) {
      if (!isAbsolute(root) || resolve(root) !== root || realpathSync(root) !== root || !lstatSync(root).isDirectory()) throw Error();
      roots.push(root);
    }
    const contained = (path: string) => roots.some(root => inside(path, root));
    const metadataVisited = new Set<string>();
    const metadataPath = (path: string) => {
      if (!contained(path) || realpathSync(path) !== path || lstatSync(path).isSymbolicLink()) throw Error();
      return path;
    };
    const metadataTree = (path: string) => {
      metadataPath(path);
      if (metadataVisited.has(path)) return;
      metadataVisited.add(path);
      if (metadataVisited.size + visited.size > 100_000) throw Error();
      const stat = lstatSync(path);
      if (stat.isDirectory()) {
        for (const name of readdirSync(path)) metadataTree(join(path, name));
      } else if (!stat.isFile() || path.endsWith('/objects/info/alternates')
        || path.endsWith('/objects/info/http-alternates') || path.endsWith('.promisor')) throw Error();
    };
    const metadataPointer = (path: string): string => {
      metadataPath(path); const stat = lstatSync(path);
      if (!stat.isFile() || stat.size > 4096) throw Error();
      const value = readFileSync(path, 'utf8').replace(/\r?\n$/, '');
      if (!value || /[\r\n\0]/.test(value)) throw Error();
      return value;
    };

    const walk = (path: string) => {
      if (excludedSqlitePaths.some(p => path === p || path === `${p}-wal` || path === `${p}-shm`)) return;
      if (visited.has(path)) return; visited.add(path);
      if (visited.size > 100_000) throw Error();
      const stat = lstatSync(path); if (stat.isSymbolicLink()) throw Error();
      if (stat.isDirectory()) {
        const names = readdirSync(path).sort();
        if (names.includes('.git')) repositories.add(path);
        for (const name of names) {
          // Git's object store is independently represented by all refs and HEAD below.
          if (name !== '.git') walk(join(path, name));
        }
      } else if (stat.isFile()) {
        bytes += stat.size; if (stat.size > 64 * 1024 * 1024 || bytes > 512 * 1024 * 1024) throw Error();
        const content = readFileSync(path); const after = lstatSync(path);
        if (stat.ino !== after.ino || stat.size !== after.size || stat.mtimeMs !== after.mtimeMs) throw Error();
        files.push({ path, bytes: content.length, sha256: createHash('sha256').update(content).digest('hex') });
      } else throw Error();
    };
    for (const root of roots) walk(root);
    for (const cwd of declaredCwds) if (!isAbsolute(cwd) || realpathSync(cwd) !== cwd || !contained(cwd) || !lstatSync(cwd).isDirectory()) throw Error();
    for (const repo of repositories) {
      // Audit actual metadata/object custody before Git is allowed to read any objects.
      const marker = metadataPath(join(repo, '.git'));
      let gitDir = marker;
      if (lstatSync(marker).isFile()) {
        const pointer = metadataPointer(marker);
        if (!pointer.startsWith('gitdir: ')) throw Error();
        gitDir = metadataPath(resolve(repo, pointer.slice(8)));
      }
      const commonPointer = join(gitDir, 'commondir');
      const commonDir = existsSync(commonPointer) ? metadataPath(resolve(gitDir, metadataPointer(commonPointer))) : gitDir;
      metadataTree(commonDir); metadataTree(gitDir);
      if (realpathSync(git(repo, ['rev-parse', '--show-toplevel']).trim()) !== repo
        || realpathSync(git(repo, ['rev-parse', '--absolute-git-dir']).trim()) !== gitDir
        || realpathSync(git(repo, ['rev-parse', '--path-format=absolute', '--git-common-dir']).trim()) !== commonDir) throw Error();
      const configuration = git(repo, ['config', '--includes', '--null', '--list']).split('\0');
      // A clean/process filter can execute during status. Unknown converters retain the VM.
      if (configuration.some(entry => /^(filter\.|extensions\.partialclone$|remote\..*\.(promisor|partialclonefilter)$)/i.test(entry.split('\n')[0] ?? ''))) throw Error();
      if (git(repo, ['status', '--porcelain=v1', '-z', '--untracked-files=all', '--ignore-submodules=all']).length) blockers.add('git_dirty');
      const worktrees = git(repo, ['worktree', 'list', '--porcelain', '-z']).split('\0').filter(line => line.startsWith('worktree ')).map(line => line.slice(9));
      for (const worktree of worktrees) {
        if (!contained(worktree) || !repositories.has(worktree)) throw Error();
      }
      const head = git(repo, ['rev-parse', '--verify', 'HEAD']).trim();
      if (!/^[a-f0-9]{40,64}$/.test(head)) throw Error();
      refs.push({ repo, ref: 'HEAD', sha: head });
      for (const line of git(repo, ['for-each-ref', '--format=%(refname) %(objectname)']).trim().split('\n').filter(Boolean)) {
        const [ref, sha] = line.split(' '); if (!ref || !sha || !/^[a-f0-9]{40,64}$/.test(sha)) throw Error(); refs.push({ repo, ref, sha });
      }
      // Unreachable objects/reflogs can contain unsaved work even when every branch is pushed.
      const unreachable = git(repo, ['fsck', '--no-reflogs', '--unreachable', '--no-progress']);
      if (unreachable.trim()) blockers.add('git_unreachable');
    }
  } catch { blockers.add('filesystem_incomplete'); }
  files.sort((a,b) => a.path.localeCompare(b.path)); refs.sort((a,b) => `${a.repo}:${a.ref}`.localeCompare(`${b.repo}:${b.ref}`));
  const list = [...blockers].sort();
  return { complete: !blockers.has('filesystem_incomplete'), blockers: list, files, refs, digest: workerDigest({ files, refs }) };
}
export function verifyWorkerCheckpoint(proof: WorkerFilesystemProof, checkpoint: WorkerCheckpoint | null): boolean {
  return proof.complete && proof.blockers.length === 0 && checkpoint !== null && checkpoint.digest === proof.digest
    && workerDigest({ files: checkpoint.files, refs: checkpoint.refs }) === proof.digest;
}
export function inspectElasticWorker(options: { control: ElasticWorkerControl; ownedRoots: string[]; declaredCwds: string[];
  processes: WorkerProcessProof; checkpoint?: WorkerCheckpoint | null; now?: number; sqlitePaths?: string[] }): ElasticWorkerProbe {
  const state = options.control.read(); if (!state) throw Error('elastic_worker_unavailable');
  const observedAt = options.now ?? Date.now(); const ledger = options.control.ledger();
  const filesystem = inspectWorkerFilesystem(options.ownedRoots, options.declaredCwds, options.sqlitePaths);
  const checkpointAcknowledged = verifyWorkerCheckpoint(filesystem, options.checkpoint ?? null);
  const process = options.processes;
  const processFresh = process.complete && process.observedAt <= observedAt && observedAt - process.observedAt <= 5_000
    && process.uid > 0 && process.cgroup.length > 0 && Number.isSafeInteger(process.processes) && process.processes >= 0;
  return { version: 1, observedAt, state, ledger, filesystem, process, checkpointAcknowledged,
    drainEligible: ledger.complete && filesystem.complete && processFresh ? ledger.clean && filesystem.blockers.length === 0 && process.idle && state.phase !== 'sealed' : null,
    clean: state.phase === 'sealed' && process.processes === 0 && ledger.clean && filesystem.complete && filesystem.blockers.length === 0 && checkpointAcknowledged && processFresh && process.idle };
}
