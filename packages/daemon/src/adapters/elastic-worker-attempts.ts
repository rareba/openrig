import { managedWorkerOperations } from './elastic-managed-operation.js';
import { nativeOwnerIdentity, nativeOwnerStopped, type ElasticNativeOwner } from './elastic-native-owner.js';
import { createHash, createPrivateKey, createPublicKey, generateKeyPairSync, randomBytes } from 'node:crypto';
import { closeSync, existsSync, openSync, readdirSync, unlinkSync, writeFileSync, fsyncSync, lstatSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { z } from 'zod';
import { CheckpointPrivateStore } from './elastic-checkpoint-private-store.js';
import { azureWorkerId } from './azure-elastic-fleet.js';
import type { EnrollmentBinding } from '../domain/elastic-enrollment.js';
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const text = z.string().min(1).max(1024).regex(/^[^\x00-\x1f]+$/);
export const attemptIdentitySchema = z.object({ resourceId: text, childResourceId: text, instanceId: z.string().regex(/^\d{1,10}$/), vmId: z.string().uuid(), operationId: text, fleetEpoch: text,
    manifestDigest: digest, artifactDigest: digest, specDigest: digest, checkoutDigest: digest, configurationDigest: digest }).strict();
export type AttemptIdentity = z.infer<typeof attemptIdentitySchema>;
const commandSchema = z.object({ name: z.string().regex(/^[a-zA-Z0-9-]{1,64}$/), operation: z.enum(managedWorkerOperations), requestDigest: digest,
    state: z.enum(['intent', 'submitted', 'succeeded', 'failed', 'unknown']) }).strict();
const attemptSchema = z.object({ version: z.literal(1), id: digest, revision: z.number().int().nonnegative(), identity: attemptIdentitySchema, binding: z.object({ version: z.literal(1), resourceId: text, vmId: z.string().uuid(), nonce: text, manifestDigest: digest, operationId: text, fleetEpoch: text }).strict(),
    workerId: text, keyDigest: digest, phase: z.enum(['reserved', 'staging', 'installed', 'prepared', 'enrolled', 'preparing-native', 'registered', 'ready', 'blocked', 'unknown']),
    command: commandSchema.nullable(), workerPublicKeyPem: z.string().max(8192).nullable(), credential: z.object({ revision: text, expiresAt: z.number().int().positive() }).strict().nullable(),
    registration: z.object({ hostId: text, baseUrl: text, selfHostId: text }).strict().nullable(), publicReason: z.string().max(128) }).strict();
export type ElasticWorkerAttempt = z.infer<typeof attemptSchema>;
export type AttemptPatch = Partial<Pick<ElasticWorkerAttempt, 'phase' | 'command' | 'workerPublicKeyPem' | 'credential' | 'registration' | 'publicReason'>>;
export function enrollmentDigest(value: unknown): string { return createHash('sha256').update(JSON.stringify(value)).digest('hex'); }
function reject(): never { throw Error('elastic_attempt_rejected'); }
/** Private append-only revisions. An interrupted lock is deliberately retained for operator reconciliation. */
export class ElasticWorkerAttemptStore {
    private readonly files: CheckpointPrivateStore;
    readonly root: string;
    constructor(root: string, private readonly owner?: ElasticNativeOwner) {
        this.files = new CheckpointPrivateStore(root); this.root = root;
        if (owner) {
            const path = join(root, 'coordinator-owner.json');
            const binding = JSON.stringify({version:1,database:owner.identity.database});
            if (existsSync(path)) { if (this.files.read(path,8192).toString() !== binding) throw Error('native_attempt_database_conflict'); }
            else this.files.write(path,binding);
        }
    }
    private directory(id: string): string { return this.files.existing(digest.parse(id)); }
    createOrRead(input: AttemptIdentity): ElasticWorkerAttempt {
        this.owner?.assertHeld('fleet');
        const identity = attemptIdentitySchema.parse(input);
        if (identity.childResourceId.toLowerCase() !== `${identity.resourceId}/virtualMachines/${identity.instanceId}`.toLowerCase())
            reject();
        const id = enrollmentDigest([identity.resourceId.toLowerCase(), identity.vmId.toLowerCase(), identity.operationId, identity.fleetEpoch]);
        const dir = this.files.child(id);
        if (existsSync(join(dir, 'seed.json'))) {
            const existing = this.read(id);
            if (enrollmentDigest(existing.identity) !== enrollmentDigest(identity))
                reject();
            this.privateKey(id);
            return existing;
        }
        const lock = join(dir, 'create.lock');
        const fd = openSync(lock, 'wx', 0o600);
        try { writeFileSync(fd, JSON.stringify(nativeOwnerIdentity(lock))); fsyncSync(fd); } catch { /* A legacy/partial lock remains nonrecoverable. */ }
        closeSync(fd);
        this.files.sync(dir);
        try {
            const key = generateKeyPairSync('rsa', { modulusLength: 3072 });
            const privateKeyPem = key.privateKey.export({ format: 'pem', type: 'pkcs8' }).toString();
            const publicKeyPem = key.publicKey.export({ format: 'pem', type: 'spki' }).toString();
            const binding: EnrollmentBinding = { version: 1, resourceId: identity.resourceId, vmId: identity.vmId, nonce: randomBytes(32).toString('base64'), manifestDigest: identity.manifestDigest, operationId: identity.operationId, fleetEpoch: identity.fleetEpoch };
            const record: ElasticWorkerAttempt = { version: 1, id, revision: 0, identity, binding, workerId: azureWorkerId(identity.resourceId, identity.vmId), keyDigest: enrollmentDigest(publicKeyPem), phase: 'reserved', command: null, workerPublicKeyPem: null, credential: null, registration: null, publicReason: 'reserved' };
            // One publication carries both key and binding: no metadata-only state can lose its key.
            this.files.write(join(dir, 'seed.json'), JSON.stringify({ record, privateKeyPem }));
            this.files.write(join(dir, 'bearer-empty'), '');
            return this.read(id);
        }
        finally {
            unlinkSync(lock);
            this.files.sync(dir);
        }
    }
    read(id: string): ElasticWorkerAttempt {
        const dir = this.directory(id);
        const seed = z.object({ record: attemptSchema, privateKeyPem: z.string().max(8192) }).strict().parse(JSON.parse(this.files.read(join(dir, 'seed.json')).toString()));
        let record = seed.record;
        const names = readdirSync(dir).filter(n => /^revision-\d{10}\.json$/.test(n)).sort();
        for (const name of names) {
            const next = attemptSchema.parse(JSON.parse(this.files.read(join(dir, name)).toString()));
            if (next.id !== id || next.revision !== record.revision + 1 || enrollmentDigest(next.identity) !== enrollmentDigest(record.identity) || enrollmentDigest(next.binding) !== enrollmentDigest(record.binding) || next.keyDigest !== record.keyDigest)
                reject();
            record = next;
        }
        if (record.id !== id || record.keyDigest !== enrollmentDigest(createPublicKey(seed.privateKeyPem).export({ format: 'pem', type: 'spki' }).toString()))
            reject();
        return record;
    }
    advance(id: string, expectedRevision: number, patch: AttemptPatch): ElasticWorkerAttempt {
        this.owner?.assertHeld('fleet');
        const dir = this.directory(id);
        const lock = join(dir, 'advance.lock');
        const fd = openSync(lock, 'wx', 0o600);
        try { writeFileSync(fd, JSON.stringify(nativeOwnerIdentity(lock))); fsyncSync(fd); } catch { /* A legacy/partial lock remains nonrecoverable. */ }
        closeSync(fd);
        this.files.sync(dir);
        try {
            const current = this.read(id);
            if (current.revision !== expectedRevision)
                reject();
            if (current.command && !['succeeded', 'failed'].includes(current.command.state) && Object.hasOwn(patch, 'command') && (!patch.command || enrollmentDigest({ ...current.command, state: 'intent' }) !== enrollmentDigest({ ...patch.command, state: 'intent' })))
                reject();
            const next = attemptSchema.parse({ ...current, ...patch, revision: current.revision + 1 });
            this.files.write(join(dir, `revision-${String(next.revision).padStart(10, '0')}.json`), JSON.stringify(next));
            return this.read(id);
        }
        finally {
            unlinkSync(lock);
            this.files.sync(dir);
        }
    }
    list(): ElasticWorkerAttempt[] {
        this.files.directory(this.root);
        return readdirSync(this.root).filter(n => /^[a-f0-9]{64}$/.test(n)).map(id => this.read(id));
    }
    /** Only a production store constructed with the SAME fleet owner can recover locks.
     * Unknown/legacy/other-host locks remain retained; no elapsed-time takeover exists. */
    recoverStoppedLocks(): void {
        if (!this.owner) throw Error('native_attempt_owner_required');
        this.owner.assertHeld('fleet');
        for (const name of readdirSync(this.root).filter(n => /^[a-f0-9]{64}$/.test(n))) {
            const dir = this.directory(name);
            for (const kind of ['create.lock', 'advance.lock']) {
                const path = join(dir, kind); if (!existsSync(path)) continue;
                const before = lstatSync(path); const bytes = this.files.read(path, 8192).toString();
                if (!nativeOwnerStopped(JSON.parse(bytes), nativeOwnerIdentity(path))) throw Error('native_attempt_owner_unproven');
                const after = lstatSync(path);
                if (before.ino !== after.ino || before.dev !== after.dev || readFileSync(path,'utf8') !== bytes) throw Error('native_attempt_owner_changed');
                unlinkSync(path); this.files.sync(dir);
            }
        }
    }
    listPending(): ElasticWorkerAttempt[] { return this.list().filter(a => a.phase !== 'ready' && a.phase !== 'blocked'); }
    privateKey(id: string): Buffer {
        const seed = z.object({ record: attemptSchema, privateKeyPem: z.string().max(8192) }).strict().parse(JSON.parse(this.files.read(join(this.directory(id), 'seed.json')).toString()));
        const key = createPrivateKey(seed.privateKeyPem);
        if (seed.record.id !== id || key.asymmetricKeyType !== 'rsa' || (key.asymmetricKeyDetails?.modulusLength ?? 0) < 3072 || seed.record.keyDigest !== enrollmentDigest(createPublicKey(key).export({ format: 'pem', type: 'spki' }).toString()))
            reject();
        return Buffer.from(seed.privateKeyPem);
    }
    publicKey(id: string): string { const key = this.privateKey(id); try {
        return createPublicKey(key).export({ format: 'pem', type: 'spki' }).toString();
    }
    finally {
        key.fill(0);
    } }
    saveBearer(id: string, token: string): void {
        if (!/^[A-Za-z0-9_-]{32,256}$/.test(token))
            reject();
        const path = join(this.directory(id), 'bearer');
        if (existsSync(path)) {
            if (this.files.read(path, 256).toString() !== token)
                reject();
            return;
        }
        this.files.write(path, token);
    }
    tokenPath(id: string): string { const dir = this.directory(id); return join(dir, existsSync(join(dir, 'bearer')) ? 'bearer' : 'bearer-empty'); }
    credentialReadback(id: string) { return this.read(id).credential; }
    registrationReadback(id: string) { return this.read(id).registration; }
    savePrivateInput(id: string, name: string, value: unknown): unknown {
        if (!/^[a-z0-9-]{1,96}$/.test(name))
            reject();
        const path = join(this.directory(id), `input-${name}.json`);
        if (existsSync(path))
            return JSON.parse(this.files.read(path, 256 * 1024).toString());
        this.files.write(path, JSON.stringify(value));
        return JSON.parse(this.files.read(path, 256 * 1024).toString());
    }
    readPrivateInput(id: string, name: string): unknown | null {
        if (!/^[a-z0-9-]{1,96}$/.test(name))
            reject();
        const path = join(this.directory(id), `input-${name}.json`);
        return existsSync(path) ? JSON.parse(this.files.read(path, 256 * 1024).toString()) : null;
    }
    /** Original key only; compatible with CheckpointCustody.registerAttempt. Caller clears returned Buffer. */
    custodyMaterial(id: string) { const a = this.read(id); return { enrollment: a.binding, controllerPrivateKeyPem: this.privateKey(id) }; }
}
