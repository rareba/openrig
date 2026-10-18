import {
  constants,
  createCipheriv,
  createHash,
  createPublicKey,
  publicEncrypt,
  randomBytes,
} from "node:crypto";
import {
  constants as fsConstants,
  closeSync,
  existsSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  openSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { join, resolve } from "node:path";
import Database from "better-sqlite3";
import { inspectCheckpointSqlite } from "./elastic-checkpoint-sqlite.js";
import { workerDigest } from "../domain/elastic-worker-control.js";
import type { WorkerRefProof } from "./elastic-worker-probe.js";
export const CHECKPOINT_LIMIT = 512 * 1024 * 1024;
export interface FinalBinding {
  workerId: string;
  vmId: string;
  manifestDigest: string;
  fleetEpoch: string;
  drainGeneration: number;
  removalOperationId: string;
}
export interface ArchiveEntry {
  path: string;
  mode: number;
  size: number;
  sha256: string;
}
export interface FinalCheckpoint {
  version: 2;
  binding: FinalBinding;
  checkpointId: string;
  manifestDigest: string;
  archivePath: string;
  archiveSha256: string;
  archiveBytes: number;
  sourceDigest: string;
  manifest: CheckpointManifest;
}
export interface CheckpointManifest {
  version: 2;
  binding: FinalBinding;
  entries: ArchiveEntry[];
  gitRefs: WorkerRefProof[];
  sqlite: Array<{ path: string; schemaDigest: string; tables: string[] }>;
}
export interface EncryptedCheckpoint {
  version: 1;
  checkpointId: string;
  manifestDigest: string;
  binding: FinalBinding;
  ciphertextPath: string;
  ciphertextSha256: string;
  ciphertextBytes: number;
  envelope: {
    algorithm: "RSA-OAEP-SHA256/AES-256-GCM";
    wrappedKey: string;
    iv: string;
    tag: string;
    aad: string;
    controllerKeyDigest: string;
  };
}
function fail(): never {
  throw Error("worker_checkpoint_rejected");
}
export function sha256(bytes: Buffer | string): string {
  return createHash("sha256").update(bytes).digest("hex");
}
export function durableWrite(
  path: string,
  bytes: Buffer | string,
  mode = 0o600,
): void {
  const temp = `${path}.${randomBytes(8).toString("hex")}.tmp`;
  const fd = openSync(temp, "wx", mode);
  try {
    writeFileSync(fd, bytes);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temp, path);
  const dir = openSync(resolve(path, ".."), "r");
  try {
    fsyncSync(dir);
  } finally {
    closeSync(dir);
  }
}
export function noFollowRead(path: string, max = 64 * 1024 * 1024): Buffer {
  const fd = openSync(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  try {
    const a = fstatSync(fd);
    if (!a.isFile() || a.nlink !== 1 || a.size > max) fail();
    const bytes = readFileSync(fd);
    const b = fstatSync(fd);
    if (
      bytes.length !== a.size ||
      a.ino !== b.ino ||
      a.mtimeMs !== b.mtimeMs ||
      a.size !== b.size
    )
      fail();
    return bytes;
  } finally {
    closeSync(fd);
  }
}
function walkRoots(
  roots: string[],
): Array<{ path: string; source: string; mode: number; bytes: Buffer }> {
  const files: Array<{
    path: string;
    source: string;
    mode: number;
    bytes: Buffer;
  }> = [];
  let total = 0,
    count = 0;
  const seen = new Set<string>();
  for (const [index, root] of roots.entries()) {
    if (resolve(root) !== root || realpathSync(root) !== root || seen.has(root))
      fail();
    seen.add(root);
    const walk = (source: string, path: string) => {
      if (++count > 100000) fail();
      const st = lstatSync(source);
      if (st.isSymbolicLink() || realpathSync(source) !== source) fail();
      if (st.isDirectory()) {
        for (const name of readdirSync(source).sort())
          walk(join(source, name), `${path}/${name}`);
      } else {
        const bytes = noFollowRead(source);
        total += bytes.length;
        if (total > CHECKPOINT_LIMIT) fail();
        files.push({ path, source, mode: st.mode & 0o777, bytes });
      }
    };
    walk(root, `roots/${index}`);
  }
  return files;
}
export function stoppedSourceDigest(
  roots: string[],
  databases: string[] = [],
): string {
  const excluded = new Set(
    databases.flatMap((p) => [p, `${p}-wal`, `${p}-shm`]),
  );
  const files = walkRoots(roots)
    .filter((f) => !excluded.has(f.source))
    .map((f) => ({
      path: f.path,
      mode: f.mode,
      bytes: f.bytes.length,
      sha256: sha256(f.bytes),
    }));
  const logical = databases.map(path => ({path, digest: inspectCheckpointSqlite(path).logicalDigest}));
  return workerDigest({ files, logical });
}
function validArchivePath(path: string): boolean {
  return (
    path.length <= 4096 &&
    !path.includes("\\") &&
    !path.includes("\0") &&
    !path.startsWith("/") &&
    path.split("/").every((p) => p !== "" && p !== "." && p !== "..")
  );
}
const MAGIC = Buffer.from("OPENRIG-CHECKPOINT-2\n");
/** Length framed bytes; decoder returns inert buffers and has no extraction/filesystem authority. */
export function decodeCheckpointArchive(bytes: Buffer): {
  manifest: CheckpointManifest;
  files: Array<{ path: string; mode: number; bytes: Buffer }>;
} {
  if (
    bytes.length > CHECKPOINT_LIMIT + 16 * 1024 * 1024 ||
    !bytes.subarray(0, MAGIC.length).equals(MAGIC)
  )
    fail();
  let at = MAGIC.length;
  const field = () => {
    if (at + 4 > bytes.length) fail();
    const len = bytes.readUInt32BE(at);
    at += 4;
    if (len > CHECKPOINT_LIMIT || at + len > bytes.length) fail();
    const out = bytes.subarray(at, at + len);
    at += len;
    return out;
  };
  const raw = field();
  if (raw.length > 16 * 1024 * 1024) fail();
  const manifest = JSON.parse(raw.toString()) as CheckpointManifest;
  if (
    manifest.version !== 2 ||
    !Array.isArray(manifest.entries) ||
    manifest.entries.length > 100000
  )
    fail();
  const seen = new Set<string>();
  const files = manifest.entries.map((e) => {
    if (
      !validArchivePath(e.path) ||
      seen.has(e.path) ||
      !Number.isInteger(e.mode) ||
      e.mode < 0 ||
      e.mode > 0o777
    )
      fail();
    seen.add(e.path);
    const data = field();
    if (data.length !== e.size || sha256(data) !== e.sha256) fail();
    return { path: e.path, mode: e.mode, bytes: data };
  });
  if (at !== bytes.length) fail();
  return { manifest, files };
}
export async function captureFinalCheckpoint(input: {
  binding: FinalBinding;
  roots: string[];
  databases: string[];
  controlDirectory: string;
  journal: Buffer;
  audit: unknown;
  gitRefs: WorkerRefProof[];
}): Promise<FinalCheckpoint> {
  const sourceDigest = stoppedSourceDigest(input.roots, input.databases);
  const files = walkRoots(input.roots);
  const excluded = new Set(
    input.databases.flatMap((p) => [p, `${p}-wal`, `${p}-shm`]),
  );
  if (files.some(f => f.bytes.subarray(0,16).toString() === 'SQLite format 3\0' && !input.databases.includes(f.source))) fail();
  const content = files.filter((f) => !excluded.has(f.source));
  const sqlite: CheckpointManifest["sqlite"] = [];
  for (const [i, path] of input.databases.entries()) {
    if (!files.some((f) => f.source === path)) fail();
    const db = new Database(path, { readonly: true, fileMustExist: true });
    const backup = join(
      input.controlDirectory,
      `backup-${i}-${randomBytes(8).toString("hex")}.sqlite`,
    );
    try {
      await db.backup(backup);
      const checked = new Database(backup, { readonly: true });
      try {
        if (checked.pragma("integrity_check", { simple: true }) !== "ok")
          fail();
        const schema = checked
          .prepare(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
          )
          .all();
        const tables = checked
          .prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name",
          )
          .all() as Array<{ name: string }>;
        sqlite.push({
          path: `sqlite/${i}.sqlite`,
          schemaDigest: workerDigest(schema),
          tables: tables.map((t) => t.name),
        });
      } finally {
        checked.close();
      }
      content.push({
        path: `sqlite/${i}.sqlite`,
        source: path,
        mode: 0o600,
        bytes: noFollowRead(backup),
      });
    } finally {
      db.close();
      if (existsSync(backup)) unlinkSync(backup);
    }
  }
  if (input.journal.length > 16 * 1024 * 1024) fail();
  content.push(
    {
      path: "audit/journal.jsonl",
      source: "",
      mode: 0o600,
      bytes: input.journal,
    },
    {
      path: "audit/control.json",
      source: "",
      mode: 0o600,
      bytes: Buffer.from(JSON.stringify(input.audit)),
    },
  );
  const manifest: CheckpointManifest = {
    version: 2,
    binding: input.binding,
    entries: content.map((f) => ({
      path: f.path,
      mode: f.mode,
      size: f.bytes.length,
      sha256: sha256(f.bytes),
    })),
    gitRefs: input.gitRefs,
    sqlite,
  };
  const manifestDigest = workerDigest(manifest);
  const checkpointId = workerDigest({ binding: input.binding, manifestDigest });
  const framed = (b: Buffer) => {
    const len = Buffer.alloc(4);
    len.writeUInt32BE(b.length);
    return [len, b];
  };
  const archive = Buffer.concat([
    MAGIC,
    ...framed(Buffer.from(JSON.stringify(manifest))),
    ...content.flatMap((f) => framed(f.bytes)),
  ]);
  decodeCheckpointArchive(archive);
  const archivePath = join(input.controlDirectory, `${checkpointId}.archive`);
  if (stoppedSourceDigest(input.roots, input.databases) !== sourceDigest) fail();
  durableWrite(archivePath, archive);
  return {
    version: 2,
    binding: input.binding,
    checkpointId,
    manifestDigest,
    manifest,
    archivePath,
    archiveSha256: sha256(archive),
    archiveBytes: archive.length,
    sourceDigest,
  };
}
export function encryptCheckpoint(
  cp: FinalCheckpoint,
  publicKeyPem: string,
): EncryptedCheckpoint {
  const key = createPublicKey(publicKeyPem);
  if (
    key.asymmetricKeyType !== "rsa" ||
    (key.asymmetricKeyDetails?.modulusLength ?? 0) < 3072
  )
    fail();
  const keyDigest = sha256(key.export({ type: "spki", format: "der" }));
  const descriptor = `${cp.archivePath}.encrypted.json`;
  if (existsSync(descriptor)) {
    const saved = JSON.parse(
      noFollowRead(descriptor, 65536).toString(),
    ) as EncryptedCheckpoint;
    if (
      saved.checkpointId !== cp.checkpointId ||
      saved.envelope.controllerKeyDigest !== keyDigest ||
      sha256(
        noFollowRead(saved.ciphertextPath, CHECKPOINT_LIMIT + 16 * 1024 * 1024),
      ) !== saved.ciphertextSha256
    )
      fail();
    return saved;
  }
  // A crash between ciphertext and descriptor publication needs trusted reconciliation; never replace ciphertext.
  if (existsSync(`${cp.archivePath}.aesgcm`)) fail();
  const plaintext = noFollowRead(
    cp.archivePath,
    CHECKPOINT_LIMIT + 16 * 1024 * 1024,
  );
  if (sha256(plaintext) !== cp.archiveSha256) fail();
  const secret = randomBytes(32),
    iv = randomBytes(12);
  const aad = JSON.stringify({
    version: 1,
    binding: cp.binding,
    checkpointId: cp.checkpointId,
    manifestDigest: cp.manifestDigest,
    archiveSha256: cp.archiveSha256,
  });
  try {
    const cipher = createCipheriv("aes-256-gcm", secret, iv);
    cipher.setAAD(Buffer.from(aad));
    const ciphertext = Buffer.concat([
      cipher.update(plaintext),
      cipher.final(),
    ]);
    const ciphertextPath = `${cp.archivePath}.aesgcm`;
    durableWrite(ciphertextPath, ciphertext);
    const result: EncryptedCheckpoint = {
      version: 1,
      checkpointId: cp.checkpointId,
      manifestDigest: cp.manifestDigest,
      binding: cp.binding,
      ciphertextPath,
      ciphertextSha256: sha256(ciphertext),
      ciphertextBytes: ciphertext.length,
      envelope: {
        algorithm: "RSA-OAEP-SHA256/AES-256-GCM",
        wrappedKey: publicEncrypt(
          {
            key,
            padding: constants.RSA_PKCS1_OAEP_PADDING,
            oaepHash: "sha256",
          },
          secret,
        ).toString("base64"),
        iv: iv.toString("base64"),
        tag: cipher.getAuthTag().toString("base64"),
        aad,
        controllerKeyDigest: keyDigest,
      },
    };
    durableWrite(descriptor, JSON.stringify(result));
    return result;
  } finally {
    secret.fill(0);
    plaintext.fill(0);
  }
}
