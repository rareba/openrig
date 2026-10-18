import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync, linkSync, readFileSync, rmSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { materializeTree, rigLauncher, stagedTreeDigest } from '../integrations/azure-elastic/build-worker-bundle.mjs';
test('materializes canonical package-relative files, refuses links escaping closure/cycles/hardlinks', () => {
 const root = mkdtempSync(join(tmpdir(), 'bundle-'));
 try {
  const src = join(root, 'src'); mkdirSync(join(src, 'package'), { recursive: true });
  writeFileSync(join(src, 'package/entry.mjs'), "import {value} from './sibling.mjs'; process.stdout.write(value)"); writeFileSync(join(src, 'package/sibling.mjs'), "export const value='canonical'");
  symlinkSync('package', join(src, 'alias'));
  materializeTree(src, join(root, 'out'));
  assert.equal(execFileSync(process.execPath, [join(root, 'out/alias/entry.mjs')], { encoding: 'utf8' }), 'canonical');
  assert.match(stagedTreeDigest(realpathSync(join(root, 'out'))), /^[a-f0-9]{64}$/);
  assert.match(rigLauncher, /\/opt\/node\/bin\/node \/opt\/openrig\/packages\/cli\/dist\/bin-wrapper.js "\$@"/);
  symlinkSync(src, join(src, 'cycle')); assert.throws(() => materializeTree(src, join(root, 'cycle-out'))); rmSync(join(src, 'cycle'));
  symlinkSync('/etc/passwd', join(src, 'escape')); assert.throws(() => materializeTree(src, join(root, 'escape-out'))); rmSync(join(src, 'escape'));
  linkSync(join(src, 'package/sibling.mjs'), join(src, 'hard')); assert.throws(() => materializeTree(src, join(root, 'hard-out')));
 } finally { rmSync(root, { recursive: true, force: true }); }
});
test('regular rig launcher preserves argv and canonical package relative imports', () => {
 const root=mkdtempSync(join(tmpdir(),'rig-launcher-'));
 try {
  mkdirSync(join(root,'package'));writeFileSync(join(root,'package/value.mjs'),"export const value='dependency'");writeFileSync(join(root,'package/bin-wrapper.mjs'),"import{value}from'./value.mjs';process.stdout.write(JSON.stringify({value,args:process.argv.slice(2)}))");
  const launcher=join(root,'rig');writeFileSync(launcher,rigLauncher.replace('/opt/node/bin/node',process.execPath).replace('/opt/openrig/packages/cli/dist/bin-wrapper.js',join(root,'package/bin-wrapper.mjs')),{mode:0o755});
  assert.deepEqual(JSON.parse(execFileSync(launcher,['whoami','space argument'],{encoding:'utf8'})),{value:'dependency',args:['whoami','space argument']});
 }finally{rmSync(root,{recursive:true,force:true})}
});
