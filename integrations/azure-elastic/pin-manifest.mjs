// Image-build step only: hash the already staged immutable trees. No downloads or runtime changes.
import { readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { pinDigest, parseLinuxManifest, assertRootPath } from '../../packages/daemon/dist/adapters/linux-privileged-worker-boundary.js';
if (process.platform !== 'linux' || process.geteuid() !== 0 || process.argv.length !== 3) throw Error('image_pin_rejected');
assertRootPath('/etc/openrig-elastic');
const manifest = JSON.parse(readFileSync(process.argv[2], 'utf8'));
manifest.pins = manifest.pins.map(pin => ({...pin, sha256: pinDigest(pin.path, pin.kind)}));
const raw = JSON.stringify(manifest);
parseLinuxManifest(raw);
writeFileSync('/etc/openrig-elastic/manifest.json', raw, {flag:'wx', mode:0o644});
writeFileSync('/etc/openrig-elastic/manifest.sha256', createHash('sha256').update(raw).digest('hex')+'\n', {flag:'wx', mode:0o644});
