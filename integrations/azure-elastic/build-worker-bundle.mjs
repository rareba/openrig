// Run only in the approved Linux builder after npm ci && npm run build:package.
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { join, resolve, relative } from 'node:path';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';
export const rigLauncher = '#!/bin/sh\nexec /opt/node/bin/node /opt/openrig/packages/cli/dist/bin-wrapper.js "$@"\n';
export const ghLauncher = '#!/bin/sh\nexec /opt/node/bin/node /opt/openrig/packages/daemon/dist/adapters/elastic-worker-github.js "$@"\n';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
function fail() { throw Error('worker_bundle_rejected'); }
/** Copy fresh files while keeping each canonical package entry at its original relative location. */
export function materializeTree(source, target, { exclude = new Set(), closureRoot = source, filter = () => true } = {}) {
 const root = realpathSync(closureRoot); let entries = 0; let bytes = 0;
 function copy(from, to, stack) {
  const canonical = realpathSync(from);
  if (canonical !== root && !canonical.startsWith(root + '/') || stack.has(canonical)) fail();
  const s = lstatSync(canonical);
  if (++entries > 250000) fail();
  if (s.isDirectory()) {
   mkdirSync(to, { mode: 0o755 }); const next = new Set(stack); next.add(canonical);
   for (const name of readdirSync(canonical).sort()) {
    if (exclude.has(name) || name === '.bin' || !filter(relative(source, join(canonical, name)), join(canonical, name))) continue;
    copy(join(canonical, name), join(to, name), next);
   }
  } else if (s.isFile() && s.nlink === 1) {
   bytes += s.size; if (bytes > 3 * 1024 ** 3 || s.size > 512 * 1024 ** 2) fail();
   writeFileSync(to, readFileSync(canonical), { flag: 'wx', mode: s.mode & 0o111 ? 0o755 : 0o644 });
  } else fail();
 }
 copy(source, target, new Set());
}
export function stagedTreeDigest(root) {
 const rows = []; let entries = 0;
 function walk(path) {
  const stat = lstatSync(path);
  if (++entries > 150000 || realpathSync(path) !== path || stat.mode & 0o022) fail();
  if (stat.isDirectory()) {
   rows.push({ path: path.slice(root.length), mode: stat.mode & 0o777, type: 'directory' });
   for (const name of readdirSync(path).sort()) walk(join(path, name));
  } else {
   if (!stat.isFile() || stat.nlink !== 1 || stat.size > 512 * 1024 ** 2) fail();
   rows.push({ path: path.slice(root.length), mode: stat.mode & 0o777, sha256: hash(readFileSync(path)) });
  }
 }
 walk(root); return hash(Buffer.from(JSON.stringify(rows)));
}
const excluded = new Set(['.git', '.superpowers', '.env', '.npmrc', '.DS_Store', '.cache', 'coverage', 'test-results', '.codex', '.claude', '.config']);
function run(program, args, cwd, env = {}) { return execFileSync(program, args, { cwd, env: { PATH: '/usr/bin:/bin', HOME: '/nonexistent', ...env }, encoding: 'utf8', timeout: 60000, maxBuffer: 8 * 1024 ** 2 }); }
/** The package recipe is approved input; builder independently checks signed Ubuntu metadata and every .deb. */
export function verifyPackageClosure(recipe, directory) {
 if (recipe.distribution !== 'ubuntu' || recipe.release !== '24.04' || recipe.architecture !== 'amd64' || !Array.isArray(recipe.packages) || !recipe.packages.length || recipe.packages.length > 256) fail();
 const evidence = recipe.signedMetadata;
 if (!evidence || !/^[a-f0-9]{64}$/.test(evidence.keyringSha256) || !Array.isArray(evidence.releases) || !evidence.releases.length || evidence.releases.length > 8) fail();
 const fixed = name => { if (!/^[A-Za-z0-9_.+%~-]+$/.test(name)) fail(); const p = join(directory, name); const s = lstatSync(p); if (!s.isFile() || s.nlink !== 1 || realpathSync(p) !== p) fail(); return p; };
 const keyring = fixed(evidence.keyringFile);
 if (hash(readFileSync(keyring)) !== evidence.keyringSha256) fail();
 const metadata = [];
 for (const releaseBinding of evidence.releases) {
  const release = fixed(releaseBinding.file);
  if (hash(readFileSync(release)) !== releaseBinding.sha256 || !Array.isArray(releaseBinding.indexes) || releaseBinding.indexes.length > 32) fail();
  run('/usr/bin/gpgv', ['--keyring', keyring, release], directory);
  const releaseText = readFileSync(release, 'utf8');
  for (const index of releaseBinding.indexes) {
   const path = fixed(index.file); const data = readFileSync(path);
   if (!/^[A-Za-z0-9_./+-]{1,256}$/.test(index.releasePath) || index.releasePath.split('/').includes('..')) fail();
   const membership = releaseText.split('\n').some(line => { const parts = line.trim().split(/\s+/); return parts.length === 3 && parts[0] === index.sha256 && parts[1] === String(data.length) && parts[2] === index.releasePath; });
   if (hash(data) !== index.sha256 || !membership) fail();
   metadata.push(path.endsWith('.xz') ? run('/usr/bin/xz', ['--decompress', '--stdout', path], directory) : data.toString());
  }
 }
 const records = metadata.join('\n\n').split(/\n\n+/);
 for (const p of recipe.packages) {
  const path = fixed(p.file); const bytes = readFileSync(path);
  if (bytes.length !== p.bytes || hash(bytes) !== p.sha256) fail();
  const record = records.find(r => (`\n${r}\n`).includes(`\nSHA256: ${p.sha256}\n`) && (`\n${r}\n`).includes(`\nPackage: ${p.package}\n`) && (`\n${r}\n`).includes(`\nVersion: ${p.version}\n`));
  if (!record) fail();
  const actual = run('/usr/bin/dpkg-deb', ['--show', '--showformat=${Package}\t${Version}\t${Architecture}', path], directory).trim();
  if (actual !== `${p.package}\t${p.version}\t${p.architecture}` || !['amd64', 'all'].includes(p.architecture)) fail();
 }
}
export function buildWorkerBundle(config) {
 if (process.platform !== 'linux' || process.arch !== 'x64' || !/^[a-f0-9]{40}$/.test(config.commit) || !/^[a-f0-9]{64}$/.test(config.lockSha256)) fail();
 const checkout = realpathSync(config.checkout); const piRoot = realpathSync(config.piRoot);
 if (run('/usr/bin/git', ['rev-parse', 'HEAD'], checkout).trim() !== config.commit || run('/usr/bin/git', ['status', '--porcelain', '--', 'packages', 'scripts', 'integrations'], checkout).trim() || hash(readFileSync(join(checkout, 'package-lock.json'))) !== config.lockSha256) fail();
 for (const file of ['packages/daemon/dist/index.js', 'packages/daemon/dist/adapters/elastic-worker-helper.js', 'packages/daemon/dist/adapters/elastic-checkpoint-sqlite-helper.js', 'packages/daemon/dist/adapters/elastic-worker-foreground.js', 'packages/cli/dist/bin-wrapper.js', 'packages/cli/daemon/dist/index.js', 'integrations/azure-elastic/worker.rig.yaml']) if (!existsSync(join(checkout, file))) fail();
 const build = readFileSync(join(checkout, 'packages/daemon/dist/build-info.js'), 'utf8');
 if (!build.includes(config.commit) || !/dirty:\s*false/.test(build)) fail();
 const pi = JSON.parse(readFileSync(join(piRoot, 'package.json'), 'utf8'));
 const bin = typeof pi.bin === 'string' ? pi.bin : pi.bin?.pi;
 if (pi.version !== '1.0.4' || typeof bin !== 'string' || resolve(piRoot, bin) !== realpathSync(join(piRoot, bin)) || !realpathSync(join(piRoot, bin)).startsWith(piRoot + '/')) fail();
 const node = realpathSync(config.node);
 if (hash(readFileSync(node)) !== config.nodeSha256) fail();
 // ABI proof is an actual native open, not an import-only or platform string check.
 run(node, ['-e', "const D=require('better-sqlite3');const d=new D(':memory:');d.exec('CREATE TABLE abi(x INTEGER)');d.close()"], checkout);
 verifyPackageClosure(config.packageRecipe, realpathSync(config.packageDirectory));
 const stage = mkdtempSync(join(tmpdir(), 'openrig-bundle-'));
 try {
  const tracked = run('/usr/bin/git', ['ls-files', '-z'], checkout).split('\0').filter(Boolean);
  const trackedPaths = new Set(tracked);
  const trackedDirectories = new Set(tracked.flatMap(path => { const parts = path.split('/'); return parts.slice(0, -1).map((_, index) => parts.slice(0, index + 1).join('/')); }));
  const generatedRoots = ['node_modules', 'packages/daemon/dist', 'packages/daemon/context-packs', 'packages/cli/dist', 'packages/cli/daemon', 'packages/cli/ui', 'packages/cli/tui', ...['daemon','cli','ui','tui'].map(p => `packages/${p}/node_modules`), 'packages/ui/dist', 'packages/tui/dist'];
  const filter = path => trackedPaths.has(path) || trackedDirectories.has(path) || generatedRoots.some(root => path === root || path.startsWith(root + '/') || root.startsWith(path + '/'));
  materializeTree(checkout, join(stage, 'openrig'), { exclude: excluded, filter });
  materializeTree(piRoot, join(stage, 'pi'), { exclude: excluded });
  mkdirSync(join(stage, 'node/bin'), { recursive: true });
  writeFileSync(join(stage, 'node/bin/node'), readFileSync(node), { mode: 0o755 });
  writeFileSync(join(stage, 'node/bin/rig'), rigLauncher, { mode: 0o755 });
  writeFileSync(join(stage, 'node/bin/gh'), ghLauncher, { mode: 0o755 });
  mkdirSync(join(stage, 'os-packages'));
  for (const p of config.packageRecipe.packages) writeFileSync(join(stage, 'os-packages', p.file), readFileSync(join(config.packageDirectory, p.file)), { mode: 0o644 });
  const provenance = { version: 1, commit: config.commit, lockSha256: config.lockSha256, nodeSha256: config.nodeSha256, nodeVersion: run(node, ['--version'], checkout).trim(), nodeAbi: run(node, ['-p', 'process.versions.modules'], checkout).trim(), architecture: process.arch, piVersion: '1.0.4', piEntry: `/opt/pi/${bin.replace(/^\.\//, '')}`, packageRecipeDigest: hash(Buffer.from(JSON.stringify(config.packageRecipe))) };
  writeFileSync(join(stage, 'provenance.json'), JSON.stringify(provenance), { mode: 0o644 });
  run('/usr/bin/tar', ['--format=ustar', '--owner=0', '--group=0', '--numeric-owner', '--mtime=@0', '--sort=name', '-cf', resolve(config.output), '-C', stage, 'openrig', 'pi', 'node', 'os-packages', 'provenance.json'], checkout);
  const pins = ['openrig', 'pi', 'node'].map(root => ({ path: `/opt/${root}`, kind: 'tree', sha256: stagedTreeDigest(join(stage, root)) }));
  const archive = readFileSync(config.output); return { sha256: hash(archive), bytes: archive.length, provenance, pins };
 } finally { rmSync(stage, { recursive: true, force: true }); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
 try { if (process.argv.length !== 3) fail(); process.stdout.write(JSON.stringify(buildWorkerBundle(JSON.parse(readFileSync(process.argv[2], 'utf8')))) + '\n'); }
 catch { process.stderr.write('worker_bundle_rejected\n'); process.exitCode = 1; }
}
