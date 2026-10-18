import { constants as cryptoConstants, createHash, createPrivateKey, createPublicKey, randomBytes, sign, timingSafeEqual } from "node:crypto";
import { constants, closeSync, createWriteStream, existsSync, fsyncSync, linkSync, openSync, readdirSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { Transform, type Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { z } from "zod";
import { sealEnrollment, type EnrollmentBinding } from "../domain/elastic-enrollment.js";
import { workerDigest } from "../domain/elastic-worker-control.js";
import { azureWorkerId } from "./azure-elastic-fleet.js";
import { parseLinuxManifest, privateIPv4 } from "./linux-privileged-worker-boundary.js";
import { sha256, type FinalBinding } from "./elastic-worker-checkpoint.js";
import { verifyCustodyReceipt, type CheckpointCapability, type CustodyReceiptBody, type SignedCustodyReceipt } from "./elastic-checkpoint-upload.js";
import { checkpointDescriptorSchema, checkpointDigest, checkpointFailure as fail, finalBindingSchema, sameBinding, type CheckpointDescriptor } from "./elastic-checkpoint-contract.js";
import { CheckpointPrivateStore, exclusiveCollision } from "./elastic-checkpoint-private-store.js";
import { requireArchivedSqlitePlatform } from "./elastic-checkpoint-sqlite-isolation.js";
import { verifyCheckpointBytes, type CheckpointTrust, type CheckpointVerification } from "./elastic-checkpoint-verifier.js";
const instanceSchema = z.object({ version: z.literal(1), imageReference: z.string().max(1024), resourceId: z.string().max(1024), vmId: z.string().max(512), manifestDigest: checkpointDigest, fleetEpoch: z.string().max(512), operationId: z.string().max(512), privateIPv4: z.string().max(64) }).strict();
const enrollmentSchema = z.object({ version: z.literal(1), resourceId: z.string().max(1024), vmId: z.string().max(512), nonce: z.string().max(512), manifestDigest: checkpointDigest, operationId: z.string().max(512), fleetEpoch: z.string().max(512) }).strict();
const summarySchema = checkpointDescriptorSchema.omit({ version: true, binding: true, envelope: true });
const summaryOf = (d: CheckpointDescriptor): CheckpointSummary => ({ checkpointId: d.checkpointId, manifestDigest: d.manifestDigest, ciphertextSha256: d.ciphertextSha256, ciphertextBytes: d.ciphertextBytes });
export type CheckpointSummary = z.infer<typeof summarySchema>;
const attemptSchema = z.object({ version: z.literal(1), enrollment: enrollmentSchema, trust: z.object({ manifestRaw: z.string().max(65536), instance: instanceSchema }).strict(), keyDigest: checkpointDigest }).strict();
const reservationSchema = z.object({ version: z.literal(1), attemptId: checkpointDigest, binding: finalBindingSchema, summary: summarySchema, maxBytes: z.number().int().positive().max(528 * 1024 * 1024) }).strict();
const capabilitySchema = z.object({ version: z.literal(1), binding: finalBindingSchema, checkpointId: checkpointDigest, token: z.string().regex(/^[A-Za-z0-9_-]{32,256}$/), expiresAt: z.number().int().positive().safe(), maxBytes: z.number().int().positive() }).strict();
const capabilityRecordSchema = z.object({ capability: capabilitySchema, tokenSha256: checkpointDigest }).strict();
export type CustodyStatus = {
    checkpointId: string;
    phase: "reserved" | "uploaded" | "verified";
    receipt: SignedCustodyReceipt | null;
};
export interface CheckpointCustodyOptions {
    /** Existing dedicated owner0700 canonical directory on retained controller storage. */
    directory: string;
    durableStoreId: string;
    maxBytes: number;
    maxReservedBytes: number;
    maxStoredBytes: number;
    /** Opaque operator-approved retention/backup policy reference; no backup is implied. */
    retentionPolicyId: string;
    maxCheckpoints: number;
}
/** Single-controller store. Immutable records recover exact complete uploads, evidence and signatures. */
export class CheckpointCustody {
    private readonly store: CheckpointPrivateStore;
    private activeUploads = 0;
    private inflightBytes = 0;
    private capacity(extra: number): void {
        if (this.store.allocatedBytes() + this.inflightBytes + extra > this.options.maxStoredBytes)
            fail();
    }
    private readonly tlsKeyDigests = new Set<string>();
    assertSeparateTlsKey(publicKeyDer: Buffer): void {
        try {
            const digest = sha256(publicKeyDer);
            for (const name of readdirSync(this.options.directory))
                if (/^[a-f0-9]{64}$/.test(name)) {
                    const path = join(this.options.directory, name, "attempt.json");
                    if (existsSync(path) && attemptSchema.parse(this.store.json(path)).keyDigest === digest)
                        fail();
                }
            this.tlsKeyDigests.add(digest);
        }
        catch {
            return fail();
        }
    }
    constructor(private readonly options: CheckpointCustodyOptions) {
        if (!/^[A-Za-z0-9_-]{1,128}$/.test(options.retentionPolicyId) || !Number.isSafeInteger(options.maxStoredBytes) || options.maxStoredBytes < options.maxBytes || options.maxStoredBytes > 1024 ** 4 || !/^[A-Za-z0-9_-]{1,128}$/.test(options.durableStoreId) || !Number.isSafeInteger(options.maxBytes) || options.maxBytes < 1 || options.maxBytes > 528 * 1024 * 1024 || !Number.isSafeInteger(options.maxReservedBytes) || options.maxReservedBytes < options.maxBytes || !Number.isSafeInteger(options.maxCheckpoints) || options.maxCheckpoints < 1 || options.maxCheckpoints > 1024)
            fail();
        this.store = (() => {
            try {
                return new CheckpointPrivateStore(options.directory);
            }
            catch {
                return fail();
            }
        })();
    }
    private exact(path: string, bytes: Buffer | string): void {
        const raw = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
        if (existsSync(path)) {
            if (!this.store.read(path, Math.max(raw.length, 128 * 1024)).equals(raw))
                fail();
            return;
        }
        try {
            this.store.write(path, raw);
        }
        catch (error) {
            if (!exclusiveCollision(error))
                throw error;
            if (!this.store.read(path, Math.max(raw.length, 128 * 1024)).equals(raw))
                fail();
        }
    }
    registerAttempt(input: {
        enrollment: EnrollmentBinding;
        trust: CheckpointTrust;
        controllerPrivateKeyPem: Buffer;
    }): {
        attemptId: string;
        keyDigest: string;
    } {
        try {
            this.capacity(256 * 1024);
            if (readdirSync(this.options.directory).length > this.options.maxCheckpoints * 2)
                fail();
            const enrollment = enrollmentSchema.parse(input.enrollment);
            const instance = instanceSchema.parse(input.trust.instance);
            const manifest = parseLinuxManifest(input.trust.manifestRaw);
            const key = createPrivateKey(input.controllerPrivateKeyPem), publicKey = createPublicKey(key);
            // Reuse enrollment's binding and key validator rather than accepting a lookalike identity.
            sealEnrollment(Buffer.from("checkpoint-key-validation"), enrollment, publicKey);
            if (!privateIPv4(instance.privateIPv4) || manifest.ownedRoots.some(r => this.options.directory === r || this.options.directory.startsWith(r + "/")) || sha256(input.trust.manifestRaw) !== enrollment.manifestDigest || instance.manifestDigest !== enrollment.manifestDigest || instance.vmId !== enrollment.vmId || instance.resourceId !== enrollment.resourceId || instance.resourceId !== manifest.resourceId || instance.fleetEpoch !== enrollment.fleetEpoch || instance.operationId !== enrollment.operationId || instance.imageReference !== manifest.linux.imageReference)
                fail();
            const keyDigest = sha256(publicKey.export({ type: "spki", format: "der" }));
            if (this.tlsKeyDigests.has(keyDigest))
                fail();
            const attemptId = workerDigest(enrollment);
            const dir = this.store.child(attemptId);
            this.exact(join(dir, "key.pem"), key.export({ type: "pkcs8", format: "pem" }).toString());
            this.exact(join(dir, "attempt.json"), JSON.stringify({ version: 1, enrollment, trust: { manifestRaw: input.trust.manifestRaw, instance }, keyDigest }));
            return { attemptId, keyDigest };
        }
        catch {
            return fail();
        }
    }
    private attempt(id: string) {
        const dir = this.store.existing(id);
        const saved = attemptSchema.parse(this.store.json(join(dir, "attempt.json")));
        if (workerDigest(saved.enrollment) !== id)
            fail();
        return { dir, saved };
    }
    private reservation(id: string) {
        const dir = this.store.existing(id);
        const saved = reservationSchema.parse(this.store.json(join(dir, "reservation.json")));
        if (saved.summary.checkpointId !== id)
            fail();
        return { dir, saved };
    }
    /** Retry with the identical summary/attempt only. Existing live capability is returned verbatim. */
    reserve(input: {
        attemptId: string;
        binding: FinalBinding;
        summary: CheckpointSummary;
        maxBytes: number;
        expiresAt: number;
    }): CheckpointCapability {
        try {
            this.capacity(16384);
            const now = Date.now();
            const { saved: attempt } = this.attempt(input.attemptId);
            const binding = finalBindingSchema.parse(input.binding);
            const summary = summarySchema.parse(input.summary);
            if (binding.workerId !== azureWorkerId(attempt.enrollment.resourceId, attempt.enrollment.vmId) || binding.vmId !== attempt.enrollment.vmId || binding.manifestDigest !== attempt.enrollment.manifestDigest || binding.fleetEpoch !== attempt.enrollment.fleetEpoch || workerDigest({ binding, manifestDigest: summary.manifestDigest }) !== summary.checkpointId || !Number.isSafeInteger(input.maxBytes) || input.maxBytes < summary.ciphertextBytes || input.maxBytes > this.options.maxBytes || input.maxBytes > parseLinuxManifest(attempt.trust.manifestRaw).linux.upload.maxBytes || !Number.isSafeInteger(input.expiresAt) || input.expiresAt <= now || input.expiresAt > now + 3600000)
                fail();
            const dir = this.store.child(summary.checkpointId);
            const record = reservationSchema.parse({ version: 1, attemptId: input.attemptId, binding, summary, maxBytes: input.maxBytes });
            if (!existsSync(join(dir, "reservation.json"))) {
                let count = 0, total = summary.ciphertextBytes;
                for (const name of readdirSync(this.options.directory))
                    if (/^[a-f0-9]{64}$/.test(name)) {
                        const path = join(this.options.directory, name, "reservation.json");
                        if (existsSync(path)) {
                            count++;
                            total += reservationSchema.parse(this.store.json(path)).summary.ciphertextBytes;
                        }
                    }
                if (count >= this.options.maxCheckpoints || total > this.options.maxReservedBytes)
                    fail();
            }
            this.exact(join(dir, "reservation.json"), JSON.stringify(record));
            const caps = readdirSync(dir).filter(n => /^cap-[0-9]+\.json$/.test(n)).sort((a, b) => Number(a.slice(4, -5)) - Number(b.slice(4, -5)));
            for (const name of [...caps].reverse()) {
                const { capability: cap, tokenSha256 } = capabilityRecordSchema.parse(this.store.json(join(dir, name)));
                if (sha256(cap.token) !== tokenSha256)
                    fail();
                if (!sameBinding(cap.binding, binding) || cap.checkpointId !== summary.checkpointId || cap.maxBytes !== record.maxBytes)
                    fail();
                if (cap.expiresAt > now && cap.expiresAt <= now + 3600000)
                    return cap;
            }
            if (caps.length >= 64)
                fail();
            const cap: CheckpointCapability = { version: 1, binding, checkpointId: summary.checkpointId, token: randomBytes(32).toString("base64url"), expiresAt: input.expiresAt, maxBytes: input.maxBytes };
            this.exact(join(dir, `cap-${caps.length}.json`), JSON.stringify({ capability: cap, tokenSha256: sha256(cap.token) }));
            return cap;
        }
        catch {
            return fail();
        }
    }
    private authorize(id: string, token: string, descriptor: CheckpointDescriptor) {
        const { dir, saved } = this.reservation(id);
        const now = Date.now();
        if (!sameBinding(descriptor.binding, saved.binding) || descriptor.checkpointId !== id || workerDigest(summaryOf(descriptor)) !== workerDigest(saved.summary) || descriptor.ciphertextBytes > saved.maxBytes || token.length > 256)
            fail();
        const { saved: attempt } = this.attempt(saved.attemptId);
        if (descriptor.envelope.controllerKeyDigest !== attempt.keyDigest)
            fail();
        const supplied = createHash("sha256").update(token).digest();
        let valid = false;
        for (const name of readdirSync(dir).filter(n => /^cap-[0-9]+\.json$/.test(n))) {
            const { capability: cap, tokenSha256 } = capabilityRecordSchema.parse(this.store.json(join(dir, name)));
            if (sha256(cap.token) !== tokenSha256)
                fail();
            if (cap.expiresAt > now && cap.expiresAt <= now + 3600000 && sameBinding(cap.binding, saved.binding) && cap.maxBytes === saved.maxBytes && timingSafeEqual(supplied, Buffer.from(tokenSha256, "hex")))
                valid = true;
        }
        if (!valid)
            fail();
        return { dir, saved };
    }
    async receive(input: {
        checkpointId: string;
        token: string;
        descriptor: unknown;
        body: Readable;
    }): Promise<CheckpointSummary> {
        let temp: string | undefined;
        let opened = false;
        let charged = 0;
        try {
            const d = checkpointDescriptorSchema.parse(input.descriptor);
            const { dir } = this.authorize(input.checkpointId, input.token, d);
            if (this.activeUploads >= 2)
                fail();
            this.capacity(d.ciphertextBytes + 128 * 1024);
            this.inflightBytes += d.ciphertextBytes;
            charged = d.ciphertextBytes;
            this.activeUploads++;
            opened = true;
            this.exact(join(dir, "descriptor.json"), JSON.stringify(d));
            temp = join(dir, `upload-${randomBytes(16).toString("hex")}.partial`);
            const fd = openSync(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
            let size = 0;
            const hash = createHash("sha256");
            const check = new Transform({ transform(chunk: Buffer, _enc, cb) {
                    size += chunk.length;
                    if (size > d.ciphertextBytes) {
                        cb(Error("checkpoint_custody_rejected"));
                        return;
                    }
                    hash.update(chunk);
                    cb(null, chunk);
                } });
            const timer = setTimeout(() => input.body.destroy(Error("checkpoint_custody_rejected")), 30000);
            timer.unref();
            try {
                await pipeline(input.body, check, createWriteStream(temp, { fd, autoClose: false }));
                if (size !== d.ciphertextBytes || hash.digest("hex") !== d.ciphertextSha256)
                    fail();
                fsyncSync(fd);
            }
            finally {
                clearTimeout(timer);
                closeSync(fd);
            }
            const destination = join(dir, "ciphertext");
            if (!existsSync(destination)) {
                try {
                    linkSync(temp, destination);
                }
                catch (error) {
                    if (!exclusiveCollision(error))
                        throw error;
                }
            }
            unlinkSync(temp);
            temp = undefined;
            this.store.sync(dir);
            const saved = this.store.read(destination, this.options.maxBytes);
            if (saved.length !== size || sha256(saved) !== d.ciphertextSha256)
                fail();
            this.exact(join(dir, "uploaded.json"), JSON.stringify(summaryOf(d)));
            return summaryOf(d);
        }
        catch {
            return fail();
        }
        finally {
            if (temp && existsSync(temp))
                unlinkSync(temp);
            if (opened) {
                this.activeUploads--;
                this.inflightBytes -= charged;
            }
        }
    }
    private descriptor(id: string): {
        dir: string;
        descriptor: CheckpointDescriptor;
    } {
        const { dir, saved } = this.reservation(id);
        const descriptor = checkpointDescriptorSchema.parse(this.store.json(join(dir, "descriptor.json")));
        if (!sameBinding(descriptor.binding, saved.binding) || workerDigest(summaryOf(descriptor)) !== workerDigest(saved.summary) || workerDigest(this.store.json(join(dir, "uploaded.json"))) !== workerDigest(saved.summary))
            fail();
        const ciphertext = this.store.read(join(dir, "ciphertext"), this.options.maxBytes);
        if (ciphertext.length !== descriptor.ciphertextBytes || sha256(ciphertext) !== descriptor.ciphertextSha256)
            fail();
        return { dir, descriptor };
    }
    verify(id: string): SignedCustodyReceipt {
        requireArchivedSqlitePlatform();
        try {
            this.capacity(128 * 1024);
            const { dir, descriptor: d } = this.descriptor(id);
            const { saved } = this.reservation(id);
            const attempt = this.attempt(saved.attemptId);
            const key = this.store.read(join(attempt.dir, "key.pem"), 16384);
            let evidence: CheckpointVerification;
            try {
                const bytes = this.store.read(join(dir, "ciphertext"), this.options.maxBytes);
                evidence = verifyCheckpointBytes({ descriptor: d, trust: attempt.saved.trust, ciphertext: bytes, privateKeyPem: key, scratchParent: dir, storage: { store: this.store, ceiling: this.options.maxStoredBytes - this.inflightBytes } });
                this.exact(join(dir, "verification.json"), JSON.stringify(evidence));
                this.exact(join(dir, "verified-commit.json"), JSON.stringify({ version: 1, checkpointId: id, evidenceSha256: sha256(JSON.stringify(evidence)), descriptorSha256: sha256(JSON.stringify(d)), keyDigest: attempt.saved.keyDigest }));
                // Original per-enrollment RSA key; receipt is published only after evidence/ciphertext fsync.
                const body: CustodyReceiptBody = { version: 1, binding: d.binding, checkpointId: id, manifestDigest: d.manifestDigest, ciphertextSha256: d.ciphertextSha256, ciphertextBytes: d.ciphertextBytes, verified: true, durableStoreId: this.options.durableStoreId };
                const receiptPath = join(dir, "receipt.json");
                if (!existsSync(receiptPath)) {
                    const receipt: SignedCustodyReceipt = { body, signature: sign("sha256", Buffer.from(JSON.stringify(body)), { key: createPrivateKey(key), padding: cryptoConstants.RSA_PKCS1_PSS_PADDING, saltLength: 32 }).toString("base64") };
                    try {
                        this.store.write(receiptPath, JSON.stringify(receipt));
                    }
                    catch (error) {
                        if (!exclusiveCollision(error))
                            throw error;
                    }
                }
                return this.receipt(id)!;
            }
            finally {
                key.fill(0);
            }
        }
        catch {
            return fail();
        }
    }
    receipt(id: string): SignedCustodyReceipt | null {
        try {
            const { dir, saved } = this.reservation(id);
            if (!existsSync(join(dir, "receipt.json")))
                return null;
            const { descriptor: d } = this.descriptor(id);
            const attempt = this.attempt(saved.attemptId);
            const evidenceBytes = this.store.read(join(dir, "verification.json"));
            const evidence = JSON.parse(evidenceBytes.toString()) as CheckpointVerification;
            const commit = z.object({ version: z.literal(1), checkpointId: checkpointDigest, evidenceSha256: checkpointDigest, descriptorSha256: checkpointDigest, keyDigest: checkpointDigest }).strict().parse(this.store.json(join(dir, "verified-commit.json")));
            if (commit.checkpointId !== id || commit.evidenceSha256 !== sha256(evidenceBytes) || commit.descriptorSha256 !== sha256(JSON.stringify(d)) || commit.keyDigest !== attempt.saved.keyDigest)
                fail();
            if (evidence.checkpointId !== id || evidence.manifestDigest !== d.manifestDigest || sha256(this.store.read(join(dir, "ciphertext"), this.options.maxBytes)) !== d.ciphertextSha256)
                fail();
            const receipt = this.store.json(join(dir, "receipt.json")) as SignedCustodyReceipt;
            const key = this.store.read(join(attempt.dir, "key.pem"), 16384);
            try {
                verifyCustodyReceipt(receipt, { ...d, ciphertextPath: "" }, createPublicKey(createPrivateKey(key)).export({ type: "spki", format: "pem" }).toString());
            }
            finally {
                key.fill(0);
            }
            if (receipt.body.durableStoreId !== this.options.durableStoreId)
                fail();
            return receipt;
        }
        catch {
            return fail();
        }
    }
    status(id: string): CustodyStatus {
        try {
            const { dir } = this.reservation(id);
            const receipt = this.receipt(id);
            return { checkpointId: id, phase: receipt ? "verified" : existsSync(join(dir, "uploaded.json")) ? (this.descriptor(id), "uploaded") : "reserved", receipt };
        }
        catch {
            return fail();
        }
    }
}
