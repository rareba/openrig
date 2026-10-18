import { readdirSync, readFileSync, readlinkSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { z } from 'zod';
import { procIdentity, type LinuxProcessIdentity, type LinuxWorkerManifest } from './linux-privileged-worker-boundary.js';
const pid = z.number().int().positive();
const identity = z.object({ pid, startTime: z.string().min(1), uid: pid, ppid: z.number().int().nonnegative(), state: z.string(), cgroup: z.string(), executable: z.string(), argv: z.array(z.string()).max(1024) }).strict();
export const capacitySchema = z.object({ version: z.literal(1), observedAt: z.number().int().nonnegative(), complete: z.boolean(), uid: pid, cgroup: z.string(), processes: z.number().int().nonnegative(), inventory: z.array(identity).max(2048), seats: z.array(z.object({ sessionName: z.string(), nodeId: z.string(), generation: z.string(), cwd: z.string(), runtime: z.literal('pi'), model: z.string(), launchId: z.string(), runner: identity, child: identity, descendants: z.array(identity).max(1024), occupancy: z.enum(['free', 'occupied', 'unknown']) }).strict()).max(4) }).strict();
export type WorkerCapacity = z.infer<typeof capacitySchema>;
export type CapacitySeat = WorkerCapacity['seats'][number];
/** Account the whole dedicated UID/cgroup. Active tool descendants belong to one occupied seat;
 * they never become a blanket idle assertion or an exemption from final stop census. */
export function accountWorkerCapacity(manifest: LinuxWorkerManifest, seats: CapacitySeat[], processes: LinuxProcessIdentity[], mainPid: number, observedAt: number): WorkerCapacity {
    const cgroup = `/system.slice/${manifest.unit}`;
    const result: WorkerCapacity = { version: 1, observedAt, complete: false, uid: manifest.uid, cgroup, processes: processes.length, inventory: processes, seats };
    if (seats.length !== manifest.linux.seats.length || new Set(seats.map(s => s.sessionName)).size !== seats.length || processes.some(p => p.uid !== manifest.uid || p.cgroup !== cgroup || !p.startTime || p.state === 'Z'))
        return result;
    const byPid = new Map(processes.map(p => [p.pid, p]));
    if (byPid.size !== processes.length)
        return result;
    const main = byPid.get(mainPid);
    if (main?.executable !== manifest.linux.node || main.argv[1] !== manifest.linux.daemon)
        return result;
    const known = new Set([mainPid]);
    for (const seat of seats) {
        if (!manifest.linux.seats.includes(seat.sessionName) || seat.occupancy === 'unknown' || !seat.nodeId || !seat.generation || !seat.launchId || seat.runtime !== 'pi')
            return result;
        const { runner, child } = seat;
        if (JSON.stringify(byPid.get(runner.pid)) !== JSON.stringify(runner) || JSON.stringify(byPid.get(child.pid)) !== JSON.stringify(child)
            || known.has(runner.pid) || known.has(child.pid) || runner.pid === child.pid || runner.executable !== manifest.linux.node || runner.argv[1] !== join(dirname(manifest.linux.helper), 'pi-runner.js')
            || !runner.argv.includes('--launch-id') || runner.argv[runner.argv.indexOf('--launch-id') + 1] !== seat.launchId || child.ppid !== runner.pid || child.executable !== manifest.linux.node || child.argv[1] !== manifest.linux.pi)
            return result;
        known.add(runner.pid);
        known.add(child.pid);
    }
    for (const seat of seats)
        seat.descendants = [];
    for (const p of processes) {
        if (known.has(p.pid))
            continue;
        const ancestors = new Set<number>();
        let parent = byPid.get(p.ppid);
        while (parent && !ancestors.has(parent.pid) && !seats.some(s => s.child.pid === parent!.pid || s.runner.pid === parent!.pid)) {
            ancestors.add(parent.pid);
            parent = byPid.get(parent.ppid);
        }
        const seat = parent && seats.find(s => s.child.pid === parent!.pid || s.runner.pid === parent!.pid);
        if (seat) {
            if (seat.occupancy !== 'occupied')
                return result;
            seat.descendants.push(p);
            known.add(p.pid);
        }
        else if (p.executable === manifest.linux.tmux || (p.executable === manifest.linux.shell && p.state === 'S' && processes.some(child => known.has(child.pid) && child.ppid === p.pid)))
            known.add(p.pid);
        else
            return result;
    }
    result.complete = known.size === processes.length;
    return result;
}
export function inspectWorkerCapacity(manifest: LinuxWorkerManifest, seats: CapacitySeat[], observedAt = Date.now()): WorkerCapacity {
    const incomplete: WorkerCapacity = { version: 1, observedAt, complete: false, uid: manifest.uid, cgroup: `/system.slice/${manifest.unit}`, processes: 0, inventory: [], seats };
    try {
        const processes: LinuxProcessIdentity[] = [];
        for (const name of readdirSync('/proc')) {
            if (!/^\d+$/.test(name))
                continue;
            const uid = Number(/^Uid:\s+(\d+)/m.exec(readFileSync(`/proc/${name}/status`, 'utf8'))?.[1]);
            const groups = readFileSync(`/proc/${name}/cgroup`, 'utf8').trim().split('\n');
            if (uid === manifest.uid || groups.some(g => g === `0::${incomplete.cgroup}` || g.startsWith(`0::${incomplete.cgroup}/`)))
                processes.push(procIdentity(Number(name)));
        }
        for (const seat of seats) {
            const env = Object.fromEntries(readFileSync(`/proc/${seat.runner.pid}/environ`, 'utf8').split('\0').filter(Boolean).map(v => { const i = v.indexOf('='); return [v.slice(0, i), v.slice(i + 1)]; }));
            if (env.OPENRIG_OCCUPANT_GENERATION !== seat.generation || env.OPENRIG_NODE_ID !== seat.nodeId || env.OPENRIG_SESSION_NAME !== seat.sessionName || readlinkSync(`/proc/${seat.child.pid}/cwd`) !== seat.cwd)
                return incomplete;
        }
        const result = accountWorkerCapacity(manifest, seats, processes, process.pid, observedAt);
        if (processes.some(p => JSON.stringify(procIdentity(p.pid)) !== JSON.stringify(p)))
            return incomplete;
        return result;
    }
    catch {
        return incomplete;
    }
}
