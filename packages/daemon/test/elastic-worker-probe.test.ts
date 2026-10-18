import { afterEach, describe, expect, it } from 'vitest';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, symlinkSync, realpathSync, chmodSync, existsSync, renameSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { inspectWorkerFilesystem, verifyWorkerCheckpoint } from '../src/adapters/elastic-worker-probe.js';
const dirs: string[]=[]; afterEach(()=>dirs.splice(0).forEach(path=>rmSync(path,{recursive:true,force:true})));
function fixture() { const root=realpathSync(mkdtempSync(join(tmpdir(),'elastic-proof-')));dirs.push(root);const repo=join(root,'repo');mkdirSync(repo);const git=(...args:string[])=>execFileSync('git',args,{cwd:repo,encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();git('init','-b','main');git('config','user.name','test');git('config','user.email','test@example.invalid');writeFileSync(join(repo,'work.txt'),'saved');git('add','.');git('commit','-m','initial');return{root,repo,git}; }
describe('independent elastic filesystem custody',()=>{
 it('enumerates all business files and refs, requiring exact controller acknowledgement',()=>{const f=fixture();const result=inspectWorkerFilesystem([f.root],[f.repo]);expect(result.complete).toBe(true);expect(result.files.some(v=>v.path.endsWith('/work.txt'))).toBe(true);expect(result.refs.some(v=>v.ref==='refs/heads/main')).toBe(true);expect(verifyWorkerCheckpoint(result,null)).toBe(false);expect(verifyWorkerCheckpoint(result,{digest:result.digest,files:result.files,refs:result.refs})).toBe(true);});
 it('detects dirty, untracked and detached work without treating clean status as saved',()=>{const f=fixture();writeFileSync(join(f.repo,'work.txt'),'changed');writeFileSync(join(f.root,'business.json'),'{}');expect(inspectWorkerFilesystem([f.root],[f.repo]).blockers).toContain('git_dirty');f.git('checkout','--','work.txt');f.git('checkout','--detach');expect(inspectWorkerFilesystem([f.root],[f.repo]).refs.some(v=>v.ref==='HEAD')).toBe(true);expect(verifyWorkerCheckpoint(inspectWorkerFilesystem([f.root],[f.repo]),null)).toBe(false);});
 it('rejects missing roots, unlisted cwd, escaped worktrees and symlinks',()=>{const f=fixture();expect(inspectWorkerFilesystem([join(f.root,'missing')],[f.repo]).complete).toBe(false);expect(inspectWorkerFilesystem([f.repo],[f.root]).complete).toBe(false);symlinkSync('/etc/passwd',join(f.root,'secret'));expect(inspectWorkerFilesystem([f.root],[f.repo]).complete).toBe(false);});
 it('invalidates acknowledgement on a post-stop business artifact change',()=>{const f=fixture();const before=inspectWorkerFilesystem([f.root],[f.repo]);const ack={digest:before.digest,files:before.files,refs:before.refs};writeFileSync(join(f.root,'report.txt'),'new');expect(verifyWorkerCheckpoint(inspectWorkerFilesystem([f.root],[f.repo]),ack)).toBe(false);});
 it('includes detached secondary worktrees and rejects worktrees outside declared inventory',()=>{const f=fixture();const extra=join(f.root,'other');f.git('worktree','add','--detach',extra);const complete=inspectWorkerFilesystem([f.root],[f.repo]);expect(complete.complete).toBe(true);expect(complete.refs.some(v=>v.repo===extra&&v.ref==='HEAD')).toBe(true);expect(inspectWorkerFilesystem([f.repo],[f.repo]).complete).toBe(false);});
 it('never executes repository fsmonitor hooks or clean/process filters during root-oriented inspection',()=>{
  const f=fixture();const marker=join(f.root,'executed');const hook=join(f.root,'hook.sh');writeFileSync(hook,`#!/bin/sh\ntouch '${marker}'\n`);chmodSync(hook,0o700);
  f.git('config','core.fsmonitor',hook);inspectWorkerFilesystem([f.root],[f.repo]);expect(existsSync(marker)).toBe(false);
  f.git('config','filter.evil.clean',hook);f.git('config','filter.evil.process',hook);f.git('config','filter.evil.required','true');writeFileSync(join(f.repo,'.gitattributes'),'*.txt filter=evil\n');
  expect(inspectWorkerFilesystem([f.root],[f.repo]).complete).toBe(false);expect(existsSync(marker)).toBe(false);
 });

 it('rejects object custody in a Git directory outside the declared owned roots',()=>{
  const f=fixture();const separate=join(f.root,'objects');const work=join(f.root,'separate');mkdirSync(work);execFileSync('git',['init','--separate-git-dir',separate,work],{stdio:'pipe'});
  expect(inspectWorkerFilesystem([work],[work]).complete).toBe(false);
 });
 it('enumerates included Git configuration before running status',()=>{
  const f=fixture();const marker=join(f.root,'executed');const hook=join(f.root,'included.sh');writeFileSync(hook,`#!/bin/sh\ntouch '${marker}'\n`);chmodSync(hook,0o700);
  const include=join(f.root,'config');writeFileSync(include,`[filter "evil"]\n clean = ${hook}\n`);f.git('config','include.path',include);writeFileSync(join(f.repo,'.gitattributes'),'*.txt filter=evil\n');
  expect(inspectWorkerFilesystem([f.root],[f.repo]).complete).toBe(false);expect(existsSync(marker)).toBe(false);
 });

 it('never invokes a repository SSH helper through partial-clone lazy fetch of a missing tree',()=>{
  const f=fixture();const marker=join(f.root,'lazy-fetch-executed');const helper=join(f.root,'ssh-helper.sh');
  writeFileSync(helper,`#!/bin/sh\ntouch '${marker}'\nexit 1\n`);chmodSync(helper,0o700);
  const tree=f.git('rev-parse','HEAD^{tree}');rmSync(join(f.repo,'.git/objects',tree.slice(0,2),tree.slice(2)));
  f.git('config','extensions.partialClone','origin');f.git('config','remote.origin.promisor','true');f.git('config','remote.origin.url','ssh://example.invalid/repo');f.git('config','core.sshCommand',helper);
  writeFileSync(join(f.repo,'work.txt'),'changed');const proof=inspectWorkerFilesystem([f.root],[f.repo]);
  expect(existsSync(marker)).toBe(false);expect(proof.complete).toBe(false);expect(verifyWorkerCheckpoint(proof,proof)).toBe(false);
 });
 it.each(['symlink','alternates','http-alternates'])('rejects escaped object custody through %s before declaring completeness',(kind)=>{
  const f=fixture();const external=realpathSync(mkdtempSync(join(tmpdir(),'elastic-objects-')));dirs.push(external);const objects=join(external,'objects');
  renameSync(join(f.repo,'.git/objects'),objects);
  if(kind==='symlink')symlinkSync(objects,join(f.repo,'.git/objects'));
  else{mkdirSync(join(f.repo,'.git/objects/info'),{recursive:true});writeFileSync(join(f.repo,'.git/objects/info',kind),objects+'\n');}
  const proof=inspectWorkerFilesystem([f.root],[f.repo]);expect(proof.complete).toBe(false);expect(verifyWorkerCheckpoint(proof,proof)).toBe(false);
 });

});
