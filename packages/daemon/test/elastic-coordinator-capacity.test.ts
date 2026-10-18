import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, it, expect } from 'vitest';
import { accountWorkerCapacity, capacitySchema, type CapacitySeat } from '../src/adapters/elastic-worker-capacity.js';
import { parseLinuxManifest, type LinuxProcessIdentity } from '../src/adapters/linux-privileged-worker-boundary.js';
const raw = JSON.parse(readFileSync(resolve(import.meta.dirname, '../../../integrations/azure-elastic/manifest.example.json'), 'utf8'));
raw.pins = raw.pins.map((p: object) => ({ ...p, sha256: 'a'.repeat(64) }));
const manifest = parseLinuxManifest(JSON.stringify(raw));
function processIdentity(pid: number, ppid: number, argv: string[]): LinuxProcessIdentity { return { pid, ppid, startTime: String(pid), uid: manifest.uid, cgroup: `/system.slice/${manifest.unit}`, executable: manifest.linux.node, argv, state: 'S' }; }
export function busyCapacity() {
    const main = processIdentity(10, 1, [manifest.linux.node, manifest.linux.daemon]);
    const seats: CapacitySeat[] = ['worker-a@elastic', 'worker-b@elastic'].map((sessionName, i) => ({ sessionName, nodeId: sessionName, generation: 'generation-' + i, cwd: '/fixture/work', runtime: 'pi', model: 'fixture/model', launchId: 'launch-' + i, runner: processIdentity(20 + i * 10, 10, [manifest.linux.node, resolve(manifest.linux.helper, '../pi-runner.js'), '--launch-id', 'launch-' + i]), child: processIdentity(21 + i * 10, 20 + i * 10, [manifest.linux.node, manifest.linux.pi]), descendants: [], occupancy: i === 0 ? 'occupied' : 'free' }));
    const tool = { ...processIdentity(22, 21, ['/usr/bin/bash', '-c', 'ongoing-work']), executable: '/usr/bin/bash' };
    return { main, seats, tool, processes: [main, ...seats.flatMap(s => [s.runner, s.child]), tool] };
}
describe('live native capacity separate from drain census', () => {
    it('accounts an ongoing tool under occupied A while B remains independently free', () => { const f = busyCapacity(); const c = accountWorkerCapacity(manifest, f.seats, f.processes, 10, 10000); expect(capacitySchema.parse(c).complete).toBe(true); expect(c.seats.map(s => s.occupancy)).toEqual(['occupied', 'free']); expect(c.seats[0]!.descendants.map(p => p.pid)).toEqual([22]); });
    it('excludes escaped, unknown, duplicate, recycled or free-seat tool identities', () => { for (const kind of ['escape', 'unknown', 'duplicate', 'recycle', 'free-tool']) {
        const f = busyCapacity();
        if (kind === 'escape')
            f.tool.cgroup = '/outside';
        if (kind === 'unknown')
            f.tool.ppid = 1;
        if (kind === 'duplicate')
            f.seats[1]!.runner = f.seats[0]!.runner;
        if (kind === 'recycle')
            f.seats[0]!.child = { ...f.seats[0]!.child, startTime: 'reused' };
        if (kind === 'free-tool')
            f.tool.ppid = 31;
        expect(accountWorkerCapacity(manifest, f.seats, f.processes, 10, 10000).complete, kind).toBe(false);
    } });
});
