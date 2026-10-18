import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync, realpathSync, statSync } from 'node:fs';
import type Database from 'better-sqlite3';
import { z } from 'zod';
const nativeOwnerSchema = z.object({ version: z.literal(1), host: z.string().min(1), boot: z.string().min(1), pid: z.number().int().positive(), start: z.string().min(1), database: z.string().min(1), instance: z.string().uuid() }).strict();
export type NativeOwnerIdentity = z.infer<typeof nativeOwnerSchema>;
function psStart(pid: number): string | null {
    try {
        process.kill(pid, 0);
    }
    catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ESRCH')
            return null;
        throw Error('native_owner_unobservable');
    }
    if (process.platform === 'linux') {
        const value = readFileSync(`/proc/${pid}/stat`, 'utf8');
        const fields = value.slice(value.lastIndexOf(')') + 2).split(' ');
        if (!fields[19])
            throw Error('native_owner_unobservable');
        return fields[19];
    }
    if (process.platform === 'darwin') {
        const start = execFileSync('/bin/ps', ['-p', String(pid), '-o', 'lstart='], { env: { LC_ALL: 'C', PATH: '/usr/bin:/bin' }, encoding: 'utf8', timeout: 2000 }).trim();
        if (!start)
            throw Error('native_owner_unobservable');
        return start;
    }
    throw Error('native_owner_platform_unavailable');
}
export function nativeOwnerIdentity(databasePath: string): NativeOwnerIdentity {
    const path = realpathSync(databasePath);
    const st = statSync(path);
    if (!st.isFile())
        throw Error('native_owner_database_unavailable');
    const sysctl = (key: string) => execFileSync('/usr/sbin/sysctl', ['-n', key], { encoding: 'utf8', timeout: 2000 }).trim();
    return nativeOwnerSchema.parse({ version: 1, host: process.platform === 'linux' ? readFileSync('/etc/machine-id', 'utf8').trim() : sysctl('kern.uuid'),
        boot: process.platform === 'linux' ? readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() : sysctl('kern.boottime'), pid: process.pid, start: psStart(process.pid), database: `${path}:${st.dev}:${st.ino}`, instance: randomUUID() });
}
/** Same trusted host/boot/DB only. A different start proves PID reuse, never a guessed absent process. */
export function nativeOwnerStopped(previousValue: unknown, current: NativeOwnerIdentity): boolean {
    const previous = nativeOwnerSchema.parse(previousValue);
    if (previous.host !== current.host || previous.boot !== current.boot || previous.database !== current.database)
        return false;
    const start = psStart(previous.pid);
    return start === null || (process.platform === 'linux' && start !== previous.start);
}
/** SQLite BEGIN IMMEDIATE is the exclusive local claim/recovery boundary. No lock expires. */
export class ElasticNativeOwner {
    readonly identity: NativeOwnerIdentity;
    private held = new Map<string, string>();
    constructor(private readonly db: Database.Database, path: string) { this.identity = nativeOwnerIdentity(path); }
    assertHeld(scope: string): void {
        const owner = this.held.get(scope);
        const row = this.db.prepare('SELECT owner FROM elastic_native_owners WHERE scope=?').get(scope) as {
            owner: string;
        } | undefined;
        if (!owner || row?.owner !== owner)
            throw Error('native_owner_required');
    }
    async run<T>(scope: string, operation: () => Promise<T>): Promise<T> {
        const owner = randomUUID();
        this.db.transaction(() => {
            const prior = this.db.prepare('SELECT owner,identity FROM elastic_native_owners WHERE scope=?').get(scope) as {
                owner: string;
                identity: string;
            } | undefined;
            if (prior) {
                if (!nativeOwnerStopped(JSON.parse(prior.identity), this.identity))
                    throw Error('native_owner_live_or_unknown');
                if (scope === 'fleet')
                    this.db.prepare("INSERT INTO elastic_native_owners VALUES('fleet-prior',?,?) ON CONFLICT(scope) DO NOTHING").run(prior.owner, prior.identity);
                if (this.db.prepare('DELETE FROM elastic_native_owners WHERE scope=? AND owner=? AND identity=?').run(scope, prior.owner, prior.identity).changes !== 1)
                    throw Error('native_owner_changed');
            }
            this.db.prepare('INSERT INTO elastic_native_owners VALUES(?,?,?)').run(scope, owner, JSON.stringify(this.identity));
        }).immediate();
        this.held.set(scope, owner);
        try {
            return await operation();
        }
        finally {
            this.held.delete(scope);
            this.db.prepare('DELETE FROM elastic_native_owners WHERE scope=? AND owner=?').run(scope, owner);
        }
    }
    /** Under the native owner, clear only the exact fleet lock left by a proved stopped owner.
     * Missing/legacy metadata remains an explicit prerequisite, never an operator Boolean. */
    recoverFleet(): void {
        this.assertHeld('fleet');
        const row = this.db.prepare('SELECT reconcile_owner FROM elastic_fleet_config WHERE id=1').get() as {
            reconcile_owner: string | null;
        };
        if (!row.reconcile_owner) { this.db.prepare("DELETE FROM elastic_native_owners WHERE scope='fleet-prior'").run(); return; }
        const match = /^(\d+):/.exec(row.reconcile_owner);
        // The lock must be the one recorded with the previous native owner, not just a PID string.
        const proof = this.db.prepare("SELECT identity FROM elastic_native_owners WHERE scope='fleet-prior'").get() as {
            identity: string;
        } | undefined;
        if (!proof || !match || !nativeOwnerStopped(JSON.parse(proof.identity), this.identity))
            throw Error('native_fleet_owner_unproven');
        const prior = nativeOwnerSchema.parse(JSON.parse(proof.identity));
        if (String(prior.pid) !== match[1])
            throw Error('native_fleet_owner_mismatch');
        if (this.db.prepare('UPDATE elastic_fleet_config SET reconcile_owner=NULL WHERE id=1 AND reconcile_owner=?').run(row.reconcile_owner).changes !== 1) throw Error('native_fleet_owner_changed');
        this.db.prepare("DELETE FROM elastic_native_owners WHERE scope='fleet-prior' AND identity=?").run(proof.identity);
    }
}
