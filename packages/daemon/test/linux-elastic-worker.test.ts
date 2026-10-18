import { execFileSync } from "node:child_process";
import { describe, it, expect, afterEach } from "vitest";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  rmSync,
  symlinkSync,
  realpathSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import { generateKeyPairSync } from "node:crypto";
import {
  captureFinalCheckpoint,
  encryptCheckpoint,
  decodeCheckpointArchive,
  stoppedSourceDigest,
} from "../src/adapters/elastic-worker-checkpoint.js";
import {
  renderWorkerUnit,
  parseLinuxManifest,
  readPrivateInput,
} from "../src/adapters/linux-privileged-worker-boundary.js";
import { assertElasticEffectAllowed } from "../src/domain/elastic-worker-guard.js";
const dirs: string[] = [];
function fixture() {
  const base = realpathSync(mkdtempSync(join(tmpdir(), "elastic-linux-")));
  dirs.push(base);
  const root = join(base, "work");
  mkdirSync(root);
  const control = join(base, "control");
  mkdirSync(control);
  return { base, root, control };
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
  delete process.env.OPENRIG_ELASTIC_GUARD;
});
const binding = {
  workerId: "worker",
  vmId: "vm",
  manifestDigest: "a".repeat(64),
  fleetEpoch: "epoch",
  drainGeneration: 1,
  removalOperationId: "remove",
};
describe("stopped checkpoint bytes", () => {
  it("backs up every SQLite table and full Git metadata, and encrypts immutable retry bytes", async () => {
    const { root, control } = fixture();
    const dbPath = join(root, "runtime.sqlite");
    const db = new Database(dbPath);
    db.pragma("journal_mode=WAL");
    db.exec(
      "CREATE TABLE extra_runtime(id INTEGER, value TEXT); INSERT INTO extra_runtime VALUES(1,'shutdown-history')",
    );
    const repo = join(root, 'repo'); mkdirSync(repo);
    const git = (...args:string[]) => execFileSync('/usr/bin/git', args, {cwd:repo,encoding:'utf8'}).trim();
    git('init','-b','main');git('config','user.name','test');git('config','user.email','test@example.invalid');
    writeFileSync(join(repo,'committed.txt'),'object-backed business file');git('add','.');git('commit','-m','saved');
    const head = git('rev-parse','HEAD');
    writeFileSync(join(root, "history"), "shutdown final bytes");
    const cp = await captureFinalCheckpoint({
      binding,
      roots: [root],
      databases: [dbPath],
      controlDirectory: control,
      journal: Buffer.from("journal"),
      audit: { phase: "sealed" },
      gitRefs: [],
    });
    const decoded = decodeCheckpointArchive(readFileSync(cp.archivePath));
    expect(decoded.files.some(f=>f.path.startsWith('roots/0/repo/.git/objects/'))).toBe(true);
    const restoreRoot=join(control,'restored-git');mkdirSync(restoreRoot);
    for(const entry of decoded.files.filter(f=>f.path.startsWith('roots/0/repo/'))){const path=join(restoreRoot,entry.path.slice('roots/0/repo/'.length));mkdirSync(join(path,'..'),{recursive:true});writeFileSync(path,entry.bytes);}
    expect(execFileSync('/usr/bin/git',['rev-parse','HEAD'],{cwd:restoreRoot,encoding:'utf8'}).trim()).toBe(head);
    execFileSync('/usr/bin/git',['fsck','--full'],{cwd:restoreRoot,stdio:'pipe'});
    const backup = decoded.files.find((f) => f.path === "sqlite/0.sqlite")!;
    const restored = join(control, "restored");
    writeFileSync(restored, backup.bytes);
    const r = new Database(restored);
    expect(r.prepare("SELECT value FROM extra_runtime").get()).toEqual({
      value: "shutdown-history",
    });
    r.close();
    const beforeClose=stoppedSourceDigest([root],[dbPath]);
    db.close();
    expect(stoppedSourceDigest([root],[dbPath])).toBe(beforeClose);
    const keys = generateKeyPairSync("rsa", { modulusLength: 3072 });
    const a = encryptCheckpoint(
      cp,
      keys.publicKey.export({ type: "spki", format: "pem" }).toString(),
    );
    const b = encryptCheckpoint(
      cp,
      keys.publicKey.export({ type: "spki", format: "pem" }).toString(),
    );
    expect(a).toEqual(b);
    expect(readFileSync(a.ciphertextPath)).not.toContain(
      Buffer.from("shutdown final bytes"),
    );
  });
  it("rejects symlink artifacts and malformed archive entry paths", async () => {
    const { root, control } = fixture();
    symlinkSync("/etc/passwd", join(root, "escape"));
    await expect(
      captureFinalCheckpoint({
        binding,
        roots: [root],
        databases: [],
        controlDirectory: control,
        journal: Buffer.alloc(0),
        audit: {},
        gitRefs: [],
      }),
    ).rejects.toThrow();
    expect(() => decodeCheckpointArchive(Buffer.from("not archive"))).toThrow();
  });
});
describe("last-effect seal guard", () => {
  it("leaves standard hosts alone and rejects missing or sealed elastic identity", () => {
    expect(() => assertElasticEffectAllowed()).not.toThrow();
    const { base } = fixture();
    process.env.OPENRIG_ELASTIC_GUARD = join(base, "guard");
    expect(() => assertElasticEffectAllowed()).toThrow();
    writeFileSync(
      process.env.OPENRIG_ELASTIC_GUARD,
      JSON.stringify({ version: 1, phase: "sealed" }),
    );
    expect(() => assertElasticEffectAllowed()).toThrow();
  });
});
describe("Linux input policy", () => {
  it("rejects arbitrary/incomplete manifest and symlink input", () => {
    expect(() => parseLinuxManifest("{}")).toThrow();
    const { base } = fixture();
    const p = join(base, "input");
    symlinkSync("/etc/passwd", p);
    expect(() => readPrivateInput(p)).toThrow();
  });
  it("renders persistent tmp confinement and a foreground unit with seal condition", () => {
    const unit = renderWorkerUnit({
      linux: {
        node: "/opt/openrig/node",
        daemon: "/opt/openrig/index.js",
        helper: "/opt/openrig/helper.js",
        home: "/var/lib/openrig-elastic/home",
        database: "/var/lib/openrig-elastic/home/db.sqlite",
        stateRoot: "/var/lib/openrig-elastic/state",
        temp: "/var/lib/openrig-elastic/tmp",
        guard: "/etc/openrig-elastic/guard.json",
        environment: "/etc/openrig-elastic/worker.env",
      },
      ownedRoots: ["/var/lib/openrig-elastic"],
      uid: 490,
      gid: 490,
    } as any);
    expect(unit).toContain("KillMode=control-group");
    expect(unit).toContain("Type=exec");
    expect(unit).toContain("BindPaths=/var/lib/openrig-elastic/tmp:/tmp");
    expect(unit).toContain("ProtectSystem=strict");
    expect(unit).toContain("ExecCondition=");
    expect(unit).not.toContain("daemon start");
  });
});

describe('stable Linux manifest confinement',()=>{
 function manifest(){const input=JSON.parse(readFileSync(new URL('../../../integrations/azure-elastic/manifest.example.json',import.meta.url),'utf8'));input.pins.forEach((p:{sha256:string})=>p.sha256='a'.repeat(64));return input;}
 it('accepts the finite profile but rejects public/arbitrary upload destinations, unknown fields and missing complete source pins',()=>{
  expect(parseLinuxManifest(JSON.stringify(manifest())).linux.seats).toEqual(['worker-a@elastic','worker-b@elastic']);
  for(const mutate of [(m:any)=>m.linux.upload.origin='https://example.invalid', (m:any)=>m.linux.upload.privateIPv4='8.8.8.8', (m:any)=>m.linux.upload.routePrefix='/arbitrary/', (m:any)=>m.command='arbitrary', (m:any)=>m.pins=m.pins.filter((p:any)=>p.path!=='/opt/openrig'),(m:any)=>m.linux.temp='/tmp',(m:any)=>m.linux.imageReference='publisher:offer:sku:latest']){const m=manifest();mutate(m);expect(()=>parseLinuxManifest(JSON.stringify(m))).toThrow();}
 });
 it('refuses an undeclared SQLite runtime store',async()=>{const f=fixture();const db=new Database(join(f.root,'unexpected.db'));db.exec('CREATE TABLE hidden_state(value TEXT)');db.close();await expect(captureFinalCheckpoint({binding,roots:[f.root],databases:[],controlDirectory:f.control,journal:Buffer.alloc(0),audit:{},gitRefs:[]})).rejects.toThrow();});
});

it('refuses a mutation during stopped checkpoint capture rather than acknowledging an older archive',async()=>{const f=fixture();const history=join(f.root,'history');writeFileSync(history,'before');const audit={toJSON(){writeFileSync(history,'changed during capture');return {phase:'sealed'};}};await expect(captureFinalCheckpoint({binding,roots:[f.root],databases:[],controlDirectory:f.control,journal:Buffer.alloc(0),audit,gitRefs:[]})).rejects.toThrow();});

describe('lossless SQLite stopped custody equality', () => {
  it('distinguishes full-width integers and storage types without JSON number coercion', () => {
    const f = fixture();
    const path = join(f.root, 'wide.sqlite');
    const db = new Database(path);
    try {
      db.exec('CREATE TABLE values_exact(value); INSERT INTO values_exact VALUES(9007199254740992)');
      const digest = () => stoppedSourceDigest([f.root], [path]);
      const first = digest();
      db.exec('UPDATE values_exact SET value=9007199254740993');
      expect(digest()).not.toBe(first);
      const integer = digest();
      db.exec("UPDATE values_exact SET value='9007199254740993'");
      expect(digest()).not.toBe(integer);
      const text = digest();
      db.exec("UPDATE values_exact SET value=x'39303037313939323534373430393933'");
      expect(digest()).not.toBe(text);
      db.exec('UPDATE values_exact SET value=1');const integerOne=digest();
      db.exec('UPDATE values_exact SET value=1.0');expect(digest()).not.toBe(integerOne);
      const priorRowid=digest();db.exec('UPDATE values_exact SET rowid=9');expect(digest()).not.toBe(priorRowid);
      const priorVersion=digest();db.pragma('user_version=5');expect(digest()).not.toBe(priorVersion);
    } finally { db.close(); }
  });
});
