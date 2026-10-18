import { constants, createDecipheriv, createPrivateKey, createPublicKey, privateDecrypt } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { z } from "zod";
import Database from "better-sqlite3";
import { workerDigest, ElasticWorkerControl } from "../domain/elastic-worker-control.js";
import { parseLinuxManifest, type TrustedInstance } from "./linux-privileged-worker-boundary.js";
import { decodeCheckpointArchive, sha256, type FinalBinding } from "./elastic-worker-checkpoint.js";
import { aadSchema, canonicalBase64, checkpointFailure as fail, finalBindingSchema, sameBinding, type CheckpointDescriptor } from "./elastic-checkpoint-contract.js";
import { gitObjectBudget } from "./elastic-checkpoint-git-objects.js";
import { inspectCheckpointSqlite } from "./elastic-checkpoint-sqlite.js";
export interface CheckpointTrust {
    manifestRaw: string;
    instance: TrustedInstance;
}
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const entry = z.object({ path: z.string().max(4096), mode: z.number().int().min(0).max(0o777), size: z.number().int().nonnegative().max(64 * 1024 * 1024), sha256: hash }).strict();
const manifestSchema = z.object({ version: z.literal(2), binding: finalBindingSchema, entries: z.array(entry).max(100000), gitRefs: z.array(z.object({ repo: z.string().max(4096), ref: z.string().max(4096), sha: z.string().regex(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/) }).strict()).max(100000), sqlite: z.array(z.object({ path: z.string(), schemaDigest: hash, tables: z.array(z.string().max(4096)).max(100000) }).strict()).max(16) }).strict();
export interface CheckpointVerification {
    version: 1;
    checkpointId: string;
    manifestDigest: string;
    archiveSha256: string;
    archiveBytes: number;
    entries: number;
    sqlite: Array<{
        path: string;
        logicalDigest: string;
    }>;
    gitRepositories: number;
}
/** No archived config, hooks, helpers, transports, alternates or worktree pointers reach Git. */
function verifyGit(files: Map<string, Buffer>, refs: z.infer<typeof manifestSchema>["gitRefs"], roots: string[], scratch: string): number {
    const archivedPath = (absolute: string) => {
        const i = roots.findIndex(r => absolute.startsWith(r + "/"));
        if (i < 0 || resolve(absolute) !== absolute)
            fail();
        return `roots/${i}${absolute.slice(roots[i]!.length)}`;
    };
    const text = (p: string) => {
        const b = files.get(archivedPath(p));
        if (!b || b.length > 1024 * 1024)
            fail();
        return new TextDecoder("utf-8", { fatal: true }).decode(b);
    };
    const repos = new Set(refs.map(r => r.repo));
    for (const name of files.keys()) {
        const match = /^roots\/(\d+)\/(.*?)(?:\/)?\.git(?:\/|$)/.exec(name);
        if (match)
            repos.add(join(roots[Number(match[1])]!, match[2]!));
    }
    if (repos.size > 128)
        fail();
    let index = 0;
    const objectBudget = gitObjectBudget();
    const env = { PATH: "/usr/bin:/bin", HOME: scratch, XDG_CONFIG_HOME: scratch, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_NO_LAZY_FETCH: "1", GIT_ALLOW_PROTOCOL: "", GIT_ATTR_NOSYSTEM: "1" };
    for (const repo of [...repos].sort()) {
        const declared = refs.filter(r => r.repo === repo);
        if (declared.filter(r => r.ref === "HEAD").length !== 1 || new Set(declared.map(r => r.ref)).size !== declared.length)
            fail();
        const marker = join(repo, ".git");
        let gitDir = marker;
        if (files.has(archivedPath(marker))) {
            const pointer = text(marker);
            if (!/^gitdir: [^\0\r\n]+\n?$/.test(pointer))
                fail();
            gitDir = resolve(repo, pointer.replace(/^gitdir: /, "").trimEnd());
            archivedPath(gitDir);
        }
        const commonPointer = files.get(archivedPath(join(gitDir, "commondir")));
        let common = gitDir;
        if (commonPointer) {
            const pointer = text(join(gitDir, "commondir"));
            if (!/^[^\0\r\n]+\n?$/.test(pointer))
                fail();
            common = resolve(gitDir, pointer.trimEnd());
            archivedPath(common);
            // Linked worktree backpointer must name a saved .git marker, never a live host path.
            if (resolve(text(join(gitDir, "gitdir")).trimEnd()) !== marker)
                fail();
        }
        const target = join(scratch, `git-${index++}`);
        mkdirSync(target, { mode: 0o700 });
        mkdirSync(join(target, "refs"), { mode: 0o700 });
        mkdirSync(join(target, "objects"), { mode: 0o700 });
        const prefix = archivedPath(common) + "/";
        for (const [path, bytes] of files) {
            if (!path.startsWith(prefix))
                continue;
            const relative = path.slice(prefix.length);
            if (relative === "objects/info/alternates" || relative === "objects/info/http-alternates" || relative.endsWith(".promisor") || relative === "shallow" || relative === "info/grafts" || relative.startsWith("refs/replace/"))
                fail();
            if (!(relative.startsWith("objects/") || relative.startsWith("refs/") || relative === "packed-refs"))
                continue;
            if (relative.startsWith("objects/") && !objectBudget(relative, bytes, declared[0]!.sha.length / 2))
                continue;
            // Git metadata is inert and ordinary; reject suspicious component names before it is read.
            if (relative.split("/").some(c => c.startsWith("-") || /[\x00-\x20\\:]/.test(c)))
                fail();
            const destination = join(target, relative);
            mkdirSync(dirname(destination), { recursive: true, mode: 0o700 });
            writeFileSync(destination, bytes, { flag: "wx", mode: 0o600 });
        }
        const head = text(join(gitDir, "HEAD"));
        if (!/^(?:ref: refs\/[^\x00-\x20]+|[a-f0-9]{40}|[a-f0-9]{64})\n?$/.test(head))
            fail();
        writeFileSync(join(target, "HEAD"), head, { flag: "wx", mode: 0o600 });
        const objectFormat = declared[0]!.sha.length === 64 ? "[extensions]\nobjectFormat=sha256\n" : "";
        writeFileSync(join(target, "config"), `[core]\nbare=true\nrepositoryformatversion=${objectFormat ? 1 : 0}\n${objectFormat}`, { flag: "wx", mode: 0o600 });
        const git = (args: string[]) => execFileSync("/usr/bin/git", ["--no-optional-locks", `--git-dir=${target}`, "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "protocol.allow=never", "-c", "credential.helper=", ...args], { cwd: scratch, env, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 10000, maxBuffer: 8 * 1024 * 1024 });
        const actual = [{ ref: "HEAD", sha: git(["rev-parse", "--verify", "HEAD"]).trim() }, ...git(["for-each-ref", "--format=%(refname) %(objectname)"]).trim().split("\n").filter(Boolean).map(line => { const [ref, sha] = line.split(" "); return { ref, sha }; })];
        if (workerDigest(actual.sort((a, b) => a.ref!.localeCompare(b.ref!))) !== workerDigest(declared.map(({ ref, sha }) => ({ ref, sha })).sort((a, b) => a.ref.localeCompare(b.ref))))
            fail();
        git(["fsck", "--full", "--strict", "--no-progress"]);
        // Every listed linked worktree must have corresponding archived metadata and declared HEAD.
        for (const path of files.keys())
            if (path.startsWith(prefix + "worktrees/") && path.endsWith("/gitdir")) {
                const pointer = new TextDecoder("utf-8", { fatal: true }).decode(files.get(path)!).trimEnd();
                if (!pointer.endsWith("/.git") || !repos.has(dirname(pointer)) || !files.has(archivedPath(pointer)))
                    fail();
            }
    }
    return repos.size;
}
export function verifyCheckpointBytes(input: {
    descriptor: CheckpointDescriptor;
    trust: CheckpointTrust;
    ciphertext: Buffer;
    privateKeyPem: Buffer;
    scratchParent: string;
}): CheckpointVerification {
    const { descriptor: d, trust } = input;
    const manifest = parseLinuxManifest(trust.manifestRaw);
    const scratch = mkdtempSync(join(input.scratchParent, "verify-"));
    let plaintext: Buffer | undefined;
    try {
        if (sha256(trust.manifestRaw) !== d.binding.manifestDigest || trust.instance.vmId !== d.binding.vmId || trust.instance.manifestDigest !== d.binding.manifestDigest || trust.instance.fleetEpoch !== d.binding.fleetEpoch || trust.instance.resourceId !== manifest.resourceId || trust.instance.imageReference !== manifest.linux.imageReference || sha256(input.ciphertext) !== d.ciphertextSha256 || input.ciphertext.length !== d.ciphertextBytes)
            fail();
        const key = createPrivateKey(input.privateKeyPem);
        if (key.asymmetricKeyType !== "rsa" || (key.asymmetricKeyDetails?.modulusLength ?? 0) < 3072 || (key.asymmetricKeyDetails?.modulusLength ?? 0) > 8192 || sha256(createPublicKey(key).export({ type: "spki", format: "der" })) !== d.envelope.controllerKeyDigest)
            fail();
        const aad = aadSchema.parse(JSON.parse(d.envelope.aad));
        if (JSON.stringify(aad) !== d.envelope.aad || !sameBinding(aad.binding, d.binding) || aad.checkpointId !== d.checkpointId || aad.manifestDigest !== d.manifestDigest)
            fail();
        const secret = privateDecrypt({ key, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: "sha256" }, canonicalBase64(d.envelope.wrappedKey, Math.ceil(key.asymmetricKeyDetails!.modulusLength! / 8)));
        try {
            if (secret.length !== 32)
                fail();
            const cipher = createDecipheriv("aes-256-gcm", secret, canonicalBase64(d.envelope.iv, 12));
            cipher.setAAD(Buffer.from(d.envelope.aad));
            cipher.setAuthTag(canonicalBase64(d.envelope.tag, 16));
            plaintext = Buffer.concat([cipher.update(input.ciphertext), cipher.final()]);
        }
        finally {
            secret.fill(0);
        }
        if (sha256(plaintext) !== aad.archiveSha256)
            fail();
        const decoded = decodeCheckpointArchive(plaintext);
        const m = manifestSchema.parse(decoded.manifest);
        if (!sameBinding(m.binding, d.binding) || workerDigest(decoded.manifest) !== d.manifestDigest || workerDigest({ binding: d.binding, manifestDigest: d.manifestDigest }) !== d.checkpointId)
            fail();
        const files = new Map(decoded.files.map(f => [f.path, f.bytes]));
        let total = 0;
        for (const f of decoded.files) {
            total += f.bytes.length;
            if (total > 512 * 1024 * 1024 || f.bytes.length > 64 * 1024 * 1024)
                fail();
            const match = /^roots\/(0|[1-9][0-9]*)\/(.+)$/.exec(f.path);
            if (match) {
                const root = manifest.ownedRoots[Number(match[1])];
                if (!root)
                    fail();
                const original = join(root, match[2]!);
                if (manifest.linux.databases.some(p => original === p || original === `${p}-wal` || original === `${p}-shm`) || f.bytes.subarray(0, 16).toString() === "SQLite format 3\0")
                    fail();
            }
            else if (f.mode !== 0o600)
                fail();
            else if (!/^(sqlite\/(0|[1-9][0-9]*)\.sqlite|audit\/(control\.json|journal\.jsonl))$/.test(f.path))
                fail();
        }
        const auditBytes = files.get("audit/control.json");
        if (!auditBytes || auditBytes.length > 1024 * 1024 || !files.has("audit/journal.jsonl"))
            fail();
        const audit = z.object({ manifest: z.unknown(), identity: z.unknown(), state: z.unknown(), ledger: z.unknown(), managedIpc: z.unknown() }).strict().parse(JSON.parse(auditBytes.toString()));
        if (workerDigest(audit.manifest) !== workerDigest(manifest) || workerDigest(z.object({ version: z.literal(1), imageReference: z.string(), resourceId: z.string(), vmId: z.string(), manifestDigest: z.string(), fleetEpoch: z.string(), operationId: z.string(), privateIPv4: z.string() }).strict().parse(audit.identity)) !== workerDigest(z.object({ version: z.literal(1), imageReference: z.string(), resourceId: z.string(), vmId: z.string(), manifestDigest: z.string(), fleetEpoch: z.string(), operationId: z.string(), privateIPv4: z.string() }).strict().parse(trust.instance)))
            fail();
        if (m.sqlite.length !== manifest.linux.databases.length || files.size !== decoded.files.length)
            fail();
        const sqlite: CheckpointVerification["sqlite"] = [];
        for (const [i, source] of manifest.linux.databases.entries()) {
            const path = `sqlite/${i}.sqlite`;
            const metadata = m.sqlite[i];
            const bytes = files.get(path);
            if (!metadata || metadata.path !== path || !bytes)
                fail();
            const target = join(scratch, `${i}.sqlite`);
            writeFileSync(target, bytes, { flag: "wx", mode: 0o600 });
            const checked = inspectCheckpointSqlite(target);
            if (metadata.schemaDigest !== checked.schemaDigest || workerDigest(metadata.tables) !== workerDigest(checked.tables))
                fail();
            sqlite.push({ path, logicalDigest: checked.logicalDigest });
            if (source === manifest.linux.database) {
                const db = new Database(target, { readonly: true, fileMustExist: true });
                try {
                    db.pragma("trusted_schema=OFF");
                    const control = new ElasticWorkerControl(db);
                    const state = control.read();
                    const ledger = control.ledger();
                    if (!state || state.phase !== "sealed" || state.generation !== d.binding.drainGeneration || state.drainOperationId !== d.binding.removalOperationId || state.workerId !== d.binding.workerId || state.vmId !== d.binding.vmId || state.manifestDigest !== d.binding.manifestDigest || !ledger.complete || !ledger.clean || workerDigest(state) !== workerDigest(audit.state) || workerDigest(ledger) !== workerDigest(audit.ledger))
                        fail();
                }
                finally {
                    db.close();
                }
            }
        }
        // The archive may not introduce extra SQLite artifacts under an allowed prefix.
        if ([...files.keys()].filter(p => p.startsWith("sqlite/")).length !== m.sqlite.length)
            fail();
        const gitRepositories = verifyGit(files, m.gitRefs, manifest.ownedRoots, scratch);
        return { version: 1, checkpointId: d.checkpointId, manifestDigest: d.manifestDigest, archiveSha256: aad.archiveSha256, archiveBytes: plaintext.length, entries: files.size, sqlite, gitRepositories };
    }
    catch {
        return fail();
    }
    finally {
        plaintext?.fill(0);
        rmSync(scratch, { recursive: true, force: true });
    }
}
