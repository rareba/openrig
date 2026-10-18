import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
const fsyncFault = vi.hoisted(() => ({ suffix: "", fds: new Map<number, string>(), peakRoot: "", peak: 0, sqliteCalls: 0 }));
vi.mock("node:fs", async (original) => {
    const fs = await original<typeof import("node:fs")>();
    const measure = () => { let sum = 0; const walk = (p: string) => { const st = fs.lstatSync(p); sum += st.blocks * 512; if (st.isDirectory())
        for (const n of fs.readdirSync(p))
            walk(p + "/" + n); }; if (fsyncFault.peakRoot) {
        walk(fsyncFault.peakRoot);
        fsyncFault.peak = Math.max(fsyncFault.peak, sum);
    } };
    return { ...fs, writeFileSync: (...args: Parameters<typeof fs.writeFileSync>) => { const result = fs.writeFileSync(...args); measure(); return result; }, openSync: (...args: Parameters<typeof fs.openSync>) => { const fd = fs.openSync(...args); fsyncFault.fds.set(fd, String(args[0])); return fd; }, closeSync: (fd: number) => { fsyncFault.fds.delete(fd); return fs.closeSync(fd); }, fsyncSync: (fd: number) => { const path = fsyncFault.fds.get(fd); if (fsyncFault.suffix && path && fs.fstatSync(fd).isDirectory() && fs.existsSync(path + "/" + fsyncFault.suffix))
            throw Object.assign(Error("fixture_directory_fsync_failure"), { code: "EIO" }); return fs.fsyncSync(fd); } };
});
vi.mock("node:child_process", async (original) => { const cp = await original<typeof import("node:child_process")>(); return { ...cp, execFileSync: (...args: Parameters<typeof cp.execFileSync>) => { if (args[0] === "/usr/bin/prlimit")
        fsyncFault.sqliteCalls++; return cp.execFileSync(...args); } }; });
import { generateKeyPairSync, createPublicKey, X509Certificate, createHash, randomBytes } from "node:crypto";
import { mkdtempSync, mkdirSync, realpathSync, rmSync, writeFileSync, readFileSync, readdirSync, chmodSync, symlinkSync, existsSync, openSync, fsyncSync, closeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { deflateSync } from "node:zlib";
import { gitObjectBudget } from "../src/adapters/elastic-checkpoint-git-objects.js";
import { CheckpointPrivateStore } from "../src/adapters/elastic-checkpoint-private-store.js";
import { Readable } from "node:stream";
import { createServer, request } from "node:https";
import Database from "better-sqlite3";
import { workerDigest, ElasticWorkerControl } from "../src/domain/elastic-worker-control.js";
import { azureWorkerId } from "../src/adapters/azure-elastic-fleet.js";
import { captureFinalCheckpoint, encryptCheckpoint, sha256, decodeCheckpointArchive, type FinalCheckpoint } from "../src/adapters/elastic-worker-checkpoint.js";
import { checkpointDescriptorSchema } from "../src/adapters/elastic-checkpoint-contract.js";
import { CheckpointCustody } from "../src/adapters/elastic-checkpoint-custody.js";
import { checkpointReceiverHandler, validatePrivateCheckpointListener } from "../src/adapters/elastic-checkpoint-receiver.js";
import { verifyCheckpointBytes } from "../src/adapters/elastic-checkpoint-verifier.js";
import { parseLinuxManifest } from "../src/adapters/linux-privileged-worker-boundary.js";
import { verifyCustodyReceipt } from "../src/adapters/elastic-checkpoint-upload.js";
const dirs: string[] = [];
let privatePem: Buffer, publicPem: string, tlsKey: Buffer, tlsCert: Buffer;
function dir() { const p = realpathSync(mkdtempSync(join(tmpdir(), "checkpoint-custody-"))); dirs.push(p); return p; }
beforeAll(() => {
    const keys = generateKeyPairSync("rsa", { modulusLength: 3072 });
    privatePem = Buffer.from(keys.privateKey.export({ type: "pkcs8", format: "pem" }));
    publicPem = keys.publicKey.export({ type: "spki", format: "pem" }).toString();
    const tls = dir();
    execFileSync("/usr/bin/openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", join(tls, "key"), "-out", join(tls, "cert"), "-days", "1", "-subj", "/CN=checkpoint-test", "-addext", "subjectAltName=IP:127.0.0.1,IP:10.44.55.66"], { stdio: "pipe" });
    tlsKey = readFileSync(join(tls, "key"));
    tlsCert = readFileSync(join(tls, "cert"));
});
afterAll(() => {
    for (const d of dirs)
        rmSync(d, { recursive: true, force: true });
    privatePem.fill(0);
});
afterEach(() => { fsyncFault.suffix = ""; fsyncFault.peakRoot = ""; fsyncFault.peak = 0; fsyncFault.sqliteCalls = 0; vi.restoreAllMocks(); });
async function fixture(git = true, packed = false, linked: boolean | number = false) {
    const base = dir(), root = join(base, "worker"), control = join(base, "control"), store = join(base, "custody");
    for (const p of [root, control, store])
        mkdirSync(p, { mode: 0o700 });
    const raw = JSON.parse(readFileSync(new URL("../../../integrations/azure-elastic/manifest.example.json", import.meta.url), "utf8"));
    raw.pins.forEach((p: {
        sha256: string;
    }) => p.sha256 = "a".repeat(64));
    raw.ownedRoots = [root];
    raw.linux.home = join(root, "home");
    raw.linux.stateRoot = join(root, "home/state/pi");
    raw.linux.database = join(root, "runtime.sqlite");
    raw.linux.databases = [raw.linux.database];
    raw.linux.temp = join(root, "temp");
    const manifestRaw = JSON.stringify(raw), manifest = parseLinuxManifest(manifestRaw), manifestDigest = sha256(manifestRaw);
    const vmId = "12345678-1234-1234-1234-123456789abc", workerId = azureWorkerId(manifest.resourceId, vmId);
    const enrollment = { version: 1 as const, resourceId: manifest.resourceId, vmId, nonce: Buffer.alloc(32, 7).toString("base64"), manifestDigest, operationId: "enroll-1", fleetEpoch: "epoch-1" };
    const instance = { version: 1 as const, resourceId: manifest.resourceId, imageReference: manifest.linux.imageReference, vmId, manifestDigest, fleetEpoch: enrollment.fleetEpoch, operationId: enrollment.operationId, privateIPv4: "10.44.55.77" };
    const binding = { workerId, vmId, manifestDigest, fleetEpoch: enrollment.fleetEpoch, drainGeneration: 1, removalOperationId: "remove-1" };
    const db = new Database(raw.linux.database);
    db.pragma("journal_mode=WAL");
    db.exec("CREATE TABLE elastic_worker_control(id INTEGER PRIMARY KEY,worker_id TEXT,vm_id TEXT,manifest_digest TEXT,phase TEXT,generation INTEGER DEFAULT 0,drain_operation_id TEXT,drain_started_at INTEGER);CREATE TABLE elastic_worker_custody(qitem_id TEXT,generation INTEGER);CREATE TABLE queue_items(qitem_id TEXT,state TEXT);CREATE TABLE outbox_entries(outbox_id TEXT,delivery_state TEXT);CREATE TABLE arbitrary_shutdown(v);INSERT INTO arbitrary_shutdown VALUES(CAST(x'80' AS TEXT)),(9007199254740993),(1.5),(x'00FF'),(NULL)");
    const native = new ElasticWorkerControl(db);
    native.enroll(binding);
    native.beginDrain(binding, binding.removalOperationId);
    native.sealAdmission(binding, 1);
    const audit = { manifest, identity: instance, state: native.read(), ledger: native.ledger(), managedIpc: { phase: "quiesced", managedIpc: [] } };
    db.close();
    writeFileSync(join(root, "shutdown-history"), "last writes");
    let gitRefs: Array<{
        repo: string;
        ref: string;
        sha: string;
    }> = [];
    if (git) {
        const repo = join(root, "repo");
        mkdirSync(repo);
        const run = (...args: string[]) => execFileSync("/usr/bin/git", args, { cwd: repo, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
        run("init", "-b", "main");
        run("config", "user.name", "fixture");
        run("config", "user.email", "test@example.invalid");
        writeFileSync(join(repo, "business"), "object-backed");
        run("add", ".");
        run("commit", "-m", "unpushed commit");
        run("branch", "unpushed-business");
        const sha = run("rev-parse", "HEAD");
        gitRefs = [{ repo, ref: "HEAD", sha }, { repo, ref: "refs/heads/main", sha }, { repo, ref: "refs/heads/unpushed-business", sha }];
        const worktrees: string[] = [];
        if (typeof linked === "number") {
            writeFileSync(join(repo, "large-history"), randomBytes(1024 * 1024));
            run("add", ".");
            run("commit", "-m", "large history");
            rmSync(join(repo, "large-history"));
            run("add", ".");
            run("commit", "-m", "history retained without current large file");
        }
        for (let i = 0; i < (typeof linked === "number" ? linked : linked ? 1 : 0); i++) {
            const worktree = join(root, `linked-${i}`);
            run("worktree", "add", "-b", `linked-business-${i}`, worktree);
            worktrees.push(worktree);
        }
        const current = run("rev-parse", "HEAD");
        const allRefs = run("for-each-ref", "--format=%(refname) %(objectname)").split("\n").filter(Boolean).map(line => { const [ref, sha] = line.split(" "); return { ref: ref!, sha: sha! }; });
        gitRefs = [repo, ...worktrees].flatMap(repo => [{ repo, ref: "HEAD", sha: current }, ...allRefs.map(r => ({ repo, ...r }))]);
        if (packed)
            run("gc", "--prune=now");
    }
    const cp = await captureFinalCheckpoint({ binding, roots: [root], databases: manifest.linux.databases, controlDirectory: control, journal: Buffer.from("final-journal\n"), audit, gitRefs });
    const options = { directory: store, durableStoreId: "test-retained-custody", maxBytes: 4 * 1024 * 1024, maxReservedBytes: 16 * 1024 * 1024, maxStoredBytes: 64 * 1024 * 1024, retentionPolicyId: "fixture-retention", maxCheckpoints: 16 };
    const custody = new CheckpointCustody(options), trust = { manifestRaw, instance };
    const attempt = custody.registerAttempt({ enrollment, trust, controllerPrivateKeyPem: privatePem });
    function reserve(cp: FinalCheckpoint) { const encrypted = encryptCheckpoint(cp, publicPem); const { ciphertextPath, ...descriptor } = encrypted; const summary = { checkpointId: encrypted.checkpointId, manifestDigest: encrypted.manifestDigest, ciphertextSha256: encrypted.ciphertextSha256, ciphertextBytes: encrypted.ciphertextBytes }; const cap = custody.reserve({ attemptId: attempt.attemptId, binding, summary, maxBytes: options.maxBytes, expiresAt: Date.now() + 60000 }); return { encrypted, descriptor, summary, cap, ciphertext: readFileSync(ciphertextPath) }; }
    return { base, root, control, store, options, custody, trust, binding, enrollment, cp, attempt, reserve, ...reserve(cp) };
}
async function upload(f: Awaited<ReturnType<typeof fixture>>) { return f.custody.receive({ checkpointId: f.cp.checkpointId, token: f.cap.token, descriptor: f.descriptor, body: Readable.from([f.ciphertext]) }); }
function mutateArchive(f: Awaited<ReturnType<typeof fixture>>, mutate: (decoded: ReturnType<typeof decodeCheckpointArchive>) => void) {
    const decoded = decodeCheckpointArchive(readFileSync(f.cp.archivePath));
    mutate(decoded);
    decoded.manifest.entries = decoded.files.map(x => ({ path: x.path, mode: x.mode, size: x.bytes.length, sha256: sha256(x.bytes) }));
    const manifestDigest = workerDigest(decoded.manifest), checkpointId = workerDigest({ binding: f.binding, manifestDigest });
    const field = (b: Buffer) => { const n = Buffer.alloc(4); n.writeUInt32BE(b.length); return [n, b]; };
    const bytes = Buffer.concat([Buffer.from("OPENRIG-CHECKPOINT-2\n"), ...field(Buffer.from(JSON.stringify(decoded.manifest))), ...decoded.files.flatMap(x => field(x.bytes))]);
    const path = join(f.control, checkpointId + ".archive");
    writeFileSync(path, bytes);
    const cp = { ...f.cp, checkpointId, manifestDigest, manifest: decoded.manifest, archivePath: path, archiveSha256: sha256(bytes), archiveBytes: bytes.length };
    return { ...f, cp, ...f.reserve(cp) };
}
describe.runIf(process.platform === "linux")("concrete durable checkpoint custody (Linux OS-limited archive verifier)", () => {
    for (const suffix of ["key.pem", "attempt.json"])
        it(`propagates ${suffix} directory fsync failure and retries its barrier`, async () => {
            const f = await fixture(false);
            const path = join(f.store, f.attempt.attemptId);
            rmSync(join(path, "attempt.json"));
            rmSync(join(path, "key.pem"));
            fsyncFault.suffix = suffix;
            const register = () => f.custody.registerAttempt({ enrollment: f.enrollment, trust: f.trust, controllerPrivateKeyPem: privatePem });
            expect(register).toThrow();
            expect(register).toThrow();
            fsyncFault.suffix = "";
            expect(register()).toEqual(f.attempt);
        });
    for (const suffix of ["verification.json", "verified-commit.json", "receipt.json"])
        it(`denies custody after ${suffix} directory fsync failure until durable retry`, async () => {
            const f = await fixture(false);
            await upload(f);
            fsyncFault.suffix = suffix;
            expect(() => f.custody.verify(f.cp.checkpointId)).toThrow();
            expect(() => f.custody.verify(f.cp.checkpointId)).toThrow();
            if (existsSync(join(f.store, f.cp.checkpointId, "receipt.json")))
                expect(() => new CheckpointCustody(f.options).receipt(f.cp.checkpointId)).toThrow();
            fsyncFault.suffix = "";
            expect(f.custody.verify(f.cp.checkpointId).body.verified).toBe(true);
        });
    it("propagates a child directory parent-fsync failure", () => { const base = dir(), store = new CheckpointPrivateStore(base), id = "a".repeat(64); fsyncFault.suffix = id; expect(() => store.child(id)).toThrow(); expect(() => store.child(id)).toThrow(); fsyncFault.suffix = ""; expect(store.child(id)).toBe(join(base, id)); });
    it("charges allocated blocks and removes completed linked quarantines before the next copy", async () => {
        const f = await fixture(true, true, 20);
        await upload(f);
        const maxStoredBytes = 16 * 1024 * 1024;
        fsyncFault.peakRoot = f.store;
        const constrained = new CheckpointCustody({ ...f.options, maxStoredBytes });
        expect(constrained.verify(f.cp.checkpointId).body.verified).toBe(true);
        expect(fsyncFault.peak).toBeLessThanOrEqual(maxStoredBytes);
        const before = new CheckpointPrivateStore(f.store).allocatedBytes();
        const ceiling = before + 512 * 1024;
        fsyncFault.peak = before;
        const tooSmall = new CheckpointCustody({ ...f.options, maxBytes: f.summary.ciphertextBytes, maxStoredBytes: ceiling });
        expect(() => tooSmall.verify(f.cp.checkpointId)).toThrow();
        expect(fsyncFault.peak).toBeLessThanOrEqual(ceiling);
    });
    it("routes complete archive SQLite and native ledger inspection through the actual OS-limited helper", async () => { const f = await fixture(false); await upload(f); f.custody.verify(f.cp.checkpointId); expect(fsyncFault.sqliteCalls).toBeGreaterThan(0); });
    it("independently verifies real arbitrary SQLite values, shutdown data and unpushed Git objects, and recovers the exact signed receipt", async () => {
        const f = await fixture();
        expect(f.custody.status(f.cp.checkpointId).phase).toBe("reserved");
        await upload(f);
        expect(f.custody.status(f.cp.checkpointId)).toEqual({ checkpointId: f.cp.checkpointId, phase: "uploaded", receipt: null });
        const receipt = f.custody.verify(f.cp.checkpointId);
        verifyCustodyReceipt(receipt, f.encrypted, publicPem);
        expect(receipt.body.verified).toBe(true);
        const restarted = new CheckpointCustody(f.options);
        expect(restarted.receipt(f.cp.checkpointId)).toEqual(receipt);
        expect(restarted.verify(f.cp.checkpointId)).toEqual(receipt);
        expect(JSON.stringify(restarted.status(f.cp.checkpointId))).not.toContain(f.cap.token);
        expect(readdirSync(join(f.store, f.cp.checkpointId)).some(p => p.startsWith("verify-"))).toBe(false);
        expect(JSON.parse(readFileSync(join(f.store, f.cp.checkpointId, "verification.json"), "utf8")).gitRepositories).toBe(1);
        await upload(f);
        expect(restarted.receipt(f.cp.checkpointId)).toEqual(receipt);
    });
    it("verifies packed Git objects and absolute linked worktree mapping entirely in private quarantine", async () => { const f = await fixture(true, true, true); await upload(f); expect(f.custody.verify(f.cp.checkpointId).body.verified).toBe(true); const evidence = JSON.parse(readFileSync(join(f.store, f.cp.checkpointId, "verification.json"), "utf8")); expect(evidence.gitRepositories).toBe(2); });
    it("rejects Git pack/loose expansion and huge index declarations before Git can allocate them", () => {
        const budget = gitObjectBudget();
        expect(() => budget("objects/aa/" + "a".repeat(38), deflateSync(Buffer.from("blob 999999999999999999999999\0small")), 20)).toThrow();
        const header = Buffer.alloc(12);
        header.write("PACK");
        header.writeUInt32BE(2, 4);
        header.writeUInt32BE(1, 8);
        // A single object declaring 128MiB is refused before any zlib body is read.
        const body = Buffer.from([0xb0, 0x80, 0x80, 0x80, 0x04]);
        const pack = Buffer.concat([header, body]);
        expect(() => budget("objects/pack/pack-" + "a".repeat(40) + ".pack", Buffer.concat([pack, createHash("sha1").update(pack).digest()]), 20)).toThrow();
        const idx = Buffer.alloc(1072);
        idx.writeUInt32BE(0xff744f63);
        idx.writeUInt32BE(2, 4);
        idx.writeUInt32BE(0xffffffff, 8 + 255 * 4);
        expect(() => budget("objects/pack/pack-" + "a".repeat(40) + ".idx", idx, 20)).toThrow();
    });
    it("retains a correctly encrypted archive containing unsafe compressed Git expansion before signing", async () => { const f0 = await fixture(), f = mutateArchive(f0, d => { d.files.push({ path: "roots/0/repo/.git/objects/aa/" + "a".repeat(38), mode: 0o600, bytes: deflateSync(Buffer.from("blob 99999999999999999999999\0small")) }); }); await upload(f); expect(() => f.custody.verify(f.cp.checkpointId)).toThrow(); expect(f.custody.receipt(f.cp.checkpointId)).toBeNull(); expect(existsSync(join(f.store, f.cp.checkpointId, "verification.json"))).toBe(false); });
    it("bounds actual zlib expansion rather than relying on declared object sizes", () => { const zipped = deflateSync(Buffer.alloc(64 * 1024 * 1024 + 1)); expect(zipped.length).toBeLessThan(100000); expect(() => gitObjectBudget()("objects/aa/" + "a".repeat(38), zipped, 20)).toThrow(); });
    it("refuses corrupted persisted ciphertext or verification evidence on restart", async () => { const f = await fixture(false); await upload(f); f.custody.verify(f.cp.checkpointId); writeFileSync(join(f.store, f.cp.checkpointId, "verification.json"), "{}"); expect(() => new CheckpointCustody(f.options).receipt(f.cp.checkpointId)).toThrow(); const g = await fixture(false); await upload(g); writeFileSync(join(g.store, g.cp.checkpointId, "ciphertext"), "broken"); expect(() => new CheckpointCustody(g.options).status(g.cp.checkpointId)).toThrow(); });
    it("recovers an exact fsynced publication interrupted before removal of its private temporary link", () => { const base = dir(), store = new CheckpointPrivateStore(base), child = store.child("a".repeat(64)), target = join(child, "record"); store.write(target, "immutable"); const temporary = join(child, "pending-" + "b".repeat(32)); execFileSync("/bin/ln", [target, temporary]); expect(store.read(target).toString()).toBe("immutable"); expect(existsSync(temporary)).toBe(false); });
    it("enforces configured disk capacity and distinct TLS/enrollment signing keys", async () => { const f = await fixture(false); expect(() => f.custody.assertSeparateTlsKey(createPublicKey(privatePem).export({ type: "spki", format: "der" }))).toThrow(); const constrained = new CheckpointCustody({ ...f.options, maxStoredBytes: f.options.maxBytes }); writeFileSync(join(f.store, "quota-fill"), Buffer.alloc(f.options.maxBytes), { mode: 0o600 }); await expect(constrained.receive({ checkpointId: f.cp.checkpointId, token: f.cap.token, descriptor: f.descriptor, body: Readable.from([f.ciphertext]) })).rejects.toThrow(); expect(f.custody.receipt(f.cp.checkpointId)).toBeNull(); });
    it("authenticates before consuming a wrong-token body and rejects mismatched identity, epoch, operation and generation", async () => {
        const f = await fixture(false);
        const read = vi.fn();
        const body = new Readable({ read });
        await expect(f.custody.receive({ checkpointId: f.cp.checkpointId, token: "z".repeat(43), descriptor: f.descriptor, body })).rejects.toThrow();
        expect(read).not.toHaveBeenCalled();
        const unknown = "e".repeat(64);
        await expect(f.custody.receive({ checkpointId: unknown, token: f.cap.token, descriptor: f.descriptor, body: Readable.from([f.ciphertext]) })).rejects.toThrow();
        expect(existsSync(join(f.store, unknown))).toBe(false);
        for (const field of ["workerId", "vmId", "fleetEpoch", "removalOperationId", "drainGeneration"]) {
            const descriptor = structuredClone(f.descriptor);
            (descriptor.binding as Record<string, unknown>)[field] = field === "drainGeneration" ? 2 : "wrong";
            await expect(f.custody.receive({ checkpointId: f.cp.checkpointId, token: f.cap.token, descriptor, body: Readable.from([f.ciphertext]) })).rejects.toThrow();
        }
        expect(f.custody.receipt(f.cp.checkpointId)).toBeNull();
    });
    it("rejects truncated, oversize and wrong-hash uploads, then permits the exact retry", async () => {
        const f = await fixture(false);
        for (const bytes of [f.ciphertext.subarray(1), Buffer.concat([f.ciphertext, Buffer.from("x")]), Buffer.alloc(f.ciphertext.length)])
            await expect(f.custody.receive({ checkpointId: f.cp.checkpointId, token: f.cap.token, descriptor: f.descriptor, body: Readable.from([bytes]) })).rejects.toThrow();
        expect(f.custody.status(f.cp.checkpointId).phase).toBe("reserved");
        await upload(f);
        expect(f.custody.verify(f.cp.checkpointId).body.verified).toBe(true);
    });
    it("retains partial stream failure without uploaded or receipt records", async () => { const f = await fixture(false); const body = Readable.from((async function* () { yield f.ciphertext.subarray(0, 20); throw Error("partial-write"); })()); await expect(f.custody.receive({ checkpointId: f.cp.checkpointId, token: f.cap.token, descriptor: f.descriptor, body })).rejects.toThrow(); expect(f.custody.status(f.cp.checkpointId).phase).toBe("reserved"); expect(readdirSync(join(f.store, f.cp.checkpointId)).some(p => p.endsWith(".partial"))).toBe(false); await upload(f); });
    it("expires capabilities and only renews the identical immutable checkpoint reservation", async () => { const f = await fixture(false); vi.spyOn(Date, "now").mockReturnValue(f.cap.expiresAt + 1); await expect(upload(f)).rejects.toThrow(); const renewed = f.custody.reserve({ attemptId: f.attempt.attemptId, binding: f.binding, summary: f.summary, maxBytes: f.options.maxBytes, expiresAt: Date.now() + 60000 }); expect(renewed.token).not.toBe(f.cap.token); await f.custody.receive({ checkpointId: f.cp.checkpointId, token: renewed.token, descriptor: f.descriptor, body: Readable.from([f.ciphertext]) }); expect(() => f.custody.reserve({ attemptId: f.attempt.attemptId, binding: f.binding, summary: { ...f.summary, ciphertextSha256: "f".repeat(64) }, maxBytes: f.options.maxBytes, expiresAt: Date.now() + 60000 })).toThrow(); });
    it("denies conflicting descriptor retry without replacing original ciphertext", async () => { const f = await fixture(false); await upload(f); const altered = structuredClone(f.descriptor); altered.envelope.tag = Buffer.alloc(16).toString("base64"); await expect(f.custody.receive({ checkpointId: f.cp.checkpointId, token: f.cap.token, descriptor: altered, body: Readable.from([f.ciphertext]) })).rejects.toThrow(); expect(readFileSync(join(f.store, f.cp.checkpointId, "ciphertext"))).toEqual(f.ciphertext); });
    for (const field of ["tag", "aad", "wrappedKey", "iv"] as const)
        it(`denies independently invalid ${field} after an authenticated complete upload`, async () => {
            const f = await fixture(false);
            if (field === "aad")
                f.descriptor.envelope.aad = f.descriptor.envelope.aad.replace("epoch-1", "epoch-2");
            else {
                const old = Buffer.from(f.descriptor.envelope[field], "base64");
                old[0] ^= 1;
                f.descriptor.envelope[field] = old.toString("base64");
            }
            await upload(f);
            expect(() => f.custody.verify(f.cp.checkpointId)).toThrow();
            expect(f.custody.receipt(f.cp.checkpointId)).toBeNull();
        });
    it("denies a key reference replacement and unsafe private storage permissions", async () => { const f = await fixture(false); await upload(f); chmodSync(join(f.store, f.attempt.attemptId, "key.pem"), 0o644); expect(() => f.custody.verify(f.cp.checkpointId)).toThrow(); chmodSync(join(f.store, f.attempt.attemptId, "key.pem"), 0o600); const keypath = join(f.store, f.attempt.attemptId, "key.pem"); rmSync(keypath); symlinkSync(join(f.control, "not-a-key"), keypath); expect(() => f.custody.verify(f.cp.checkpointId)).toThrow(); chmodSync(f.store, 0o755); expect(() => new CheckpointCustody(f.options)).toThrow(); });
    it("denies wrong SQLite schema inventory and corrupted full backup", async () => {
        for (const corrupt of [false, true]) {
            const original = await fixture(false);
            const f = mutateArchive(original, d => {
                if (corrupt)
                    d.files.find(x => x.path === "sqlite/0.sqlite")!.bytes = Buffer.from("not sqlite");
                else
                    d.manifest.sqlite[0]!.tables.push("missing_table");
            });
            await upload(f);
            expect(() => f.custody.verify(f.cp.checkpointId)).toThrow();
            expect(f.custody.receipt(f.cp.checkpointId)).toBeNull();
        }
    });
    it("reads native sealed identity and unresolved custody from SQLite instead of trusting an archive clean boolean", async () => { const f0 = await fixture(false); const f = mutateArchive(f0, d => { const file = d.files.find(x => x.path === "sqlite/0.sqlite")!; const p = join(f0.control, "edit.sqlite"); writeFileSync(p, file.bytes); const db = new Database(p); db.exec("INSERT INTO queue_items VALUES('still-owned','blocked')"); db.close(); file.bytes = readFileSync(p); }); await upload(f); expect(() => f.custody.verify(f.cp.checkpointId)).toThrow(); });
    it("rejects extra roots, duplicate entries and traversal in correctly encrypted archives", async () => {
        for (const path of ["roots/9/escape", "roots/0/../escape", "audit/control.json"]) {
            const f0 = await fixture(false), f = mutateArchive(f0, d => { d.files.push({ path, mode: 0o600, bytes: Buffer.from("bad") }); });
            await upload(f);
            expect(() => f.custody.verify(f.cp.checkpointId)).toThrow();
        }
    });
    it("rejects missing Git objects even when advertised metadata and refs survive", async () => { const f0 = await fixture(), f = mutateArchive(f0, d => { const object = d.files.findIndex(x => x.path.includes("/.git/objects/") && !x.path.includes("/info/")); expect(object).toBeGreaterThan(-1); d.files.splice(object, 1); }); await upload(f); expect(() => f.custody.verify(f.cp.checkpointId)).toThrow(); });
    it("does not execute archived includes/hooks/filters or accept external object alternates", async () => { const f0 = await fixture(); const marker = join(f0.base, "executed"); const f = mutateArchive(f0, d => { const config = d.files.find(x => x.path.endsWith("/.git/config"))!; config.bytes = Buffer.from(`[include]\npath=/does/not/exist\n[core]\nfsmonitor=touch ${marker}\n[filter \"evil\"]\nprocess=touch ${marker}\n`); }); await upload(f); expect(f.custody.verify(f.cp.checkpointId).body.verified).toBe(true); expect(existsSync(marker)).toBe(false); const f1 = await fixture(), bad = mutateArchive(f1, d => { d.files.push({ path: "roots/0/repo/.git/objects/info/alternates", mode: 0o600, bytes: Buffer.from("/external/objects") }); }); await upload(bad); expect(() => bad.custody.verify(bad.cp.checkpointId)).toThrow(); });
    it("recovers durable uploaded bytes and verification after interruption before signing", async () => { const f = await fixture(false); await upload(f); const evidence = verifyCheckpointBytes({ descriptor: f.descriptor, trust: f.trust, ciphertext: f.ciphertext, privateKeyPem: privatePem, scratchParent: join(f.store, f.cp.checkpointId), storage: { store: new CheckpointPrivateStore(f.store), ceiling: f.options.maxStoredBytes } }); const p = join(f.store, f.cp.checkpointId, "verification.json"), fd = openSync(p, "wx", 0o600); writeFileSync(fd, JSON.stringify(evidence)); fsyncSync(fd); closeSync(fd); const restarted = new CheckpointCustody(f.options); expect(restarted.status(f.cp.checkpointId).phase).toBe("uploaded"); expect(restarted.verify(f.cp.checkpointId).body.verified).toBe(true); });
});
describe("bounded private TLS checkpoint receiver", () => {
    it("validates exact private bind, pinned certificate, SAN/key and expiry without a production loopback bypass", () => {
        const options = { bindIPv4: "10.44.55.66", port: 17434, certificatePem: tlsCert, tlsPrivateKeyPem: tlsKey, certificateSha256: new X509Certificate(tlsCert).fingerprint256.replaceAll(":", "").toLowerCase() };
        expect(() => validatePrivateCheckpointListener(options)).not.toThrow();
        for (const change of [{ bindIPv4: "127.0.0.1" }, { bindIPv4: "8.8.8.8" }, { bindIPv4: "10.44.55.67" }, { certificateSha256: "a".repeat(64) }, { tlsPrivateKeyPem: privatePem }])
            expect(() => validatePrivateCheckpointListener({ ...options, ...change })).toThrow();
    });
    it("handles real loopback TLS1.3 upload, strict headers/method/path and duplicate authorization", async () => {
        const f = await fixture(false);
        const server = createServer({ cert: tlsCert, key: tlsKey, minVersion: "TLSv1.3", maxHeaderSize: 20000 }, checkpointReceiverHandler(f.custody));
        await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
        const address = server.address();
        if (!address || typeof address === "string")
            throw Error("listener");
        const send = (change: {
            method?: string;
            path?: string;
            auth?: string;
            extra?: Record<string, string>;
            duplicate?: boolean;
        } = {}) => new Promise<{
            status: number;
            body: string;
        }>((resolve, reject) => { const headers: Record<string, string> | string[] = change.duplicate ? ["authorization", `Bearer ${f.cap.token}`, "authorization", `Bearer ${f.cap.token}`, "content-type", "application/octet-stream", "content-length", String(f.ciphertext.length), "x-openrig-checkpoint", Buffer.from(JSON.stringify(checkpointDescriptorSchema.parse(f.descriptor))).toString("base64")] : { authorization: `Bearer ${change.auth ?? f.cap.token}`, "content-type": "application/octet-stream", "content-length": String(f.ciphertext.length), "x-openrig-checkpoint": Buffer.from(JSON.stringify(checkpointDescriptorSchema.parse(f.descriptor))).toString("base64"), ...change.extra }; const req = request({ host: "127.0.0.1", port: address.port, method: change.method ?? "PUT", path: change.path ?? `/api/elastic-checkpoints/${f.cp.checkpointId}`, ca: tlsCert, minVersion: "TLSv1.3", headers }, res => { let body = ""; res.on("data", b => body += b); res.on("end", () => resolve({ status: res.statusCode!, body })); }); req.on("error", reject); req.end(f.ciphertext); });
        try {
            for (const change of [{ auth: "x".repeat(43) }, { method: "POST" }, { path: "/api/operator" }, { extra: { "content-encoding": "gzip" } }, { duplicate: true }])
                expect((await send(change)).status).toBe(400);
            expect(f.custody.status(f.cp.checkpointId).phase).toBe("reserved");
            expect((await send()).status).toBe(201);
            expect(f.custody.status(f.cp.checkpointId).phase).toBe("uploaded");
            expect(f.custody.receipt(f.cp.checkpointId)).toBeNull();
        }
        finally {
            await new Promise<void>((resolve, reject) => server.close(e => e ? reject(e) : resolve()));
            server.closeAllConnections();
        }
    });
});
