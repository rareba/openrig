import type { ElasticWorkerState } from '../domain/elastic-worker-control.js';
import { execFileSync } from "node:child_process";
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  readlinkSync,
  realpathSync,
  chownSync,
  chmodSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { networkInterfaces } from "node:os";
import { z } from "zod";
import type {
  PrivilegedWorkerBoundary,
  WorkerRuntimeMaterial,
} from "./azure-worker-bootstrap.js";
import type { WorkerProcessProof } from "./elastic-worker-probe.js";
import {
  durableWrite,
  noFollowRead,
  sha256,
} from "./elastic-worker-checkpoint.js";
import { piSeatPaths } from "./pi-runner-protocol.js";
import { azureWorkerId } from "./azure-elastic-fleet.js";
const path = z
  .string()
  .regex(/^\/[a-zA-Z0-9_./-]+$/)
  .refine((p) => resolve(p) === p && p !== "/");
const digest = z.string().regex(/^[a-f0-9]{64}$/);
export const linuxManifestSchema = z
  .object({
    version: z.literal(1),
    resourceId: z.string().min(1).max(1024),
    workerUser: z.literal("openrig-elastic"),
    unit: z.literal("openrig-elastic.service"),
    uid: z.number().int().min(100).max(60000),
    gid: z.number().int().min(100).max(60000),
    controlDirectory: path,
    ownedRoots: z.array(path).min(1).max(16),
    bindPolicy: z.literal("private-ipv4"),
    port: z.number().int().min(1024).max(65535),
    pins: z
      .array(
        z
          .object({ path, sha256: digest, kind: z.enum(["file", "tree"]) })
          .strict(),
      )
      .min(1)
      .max(64),
    materialFiles: z
      .object({ piAuthFile: path, piSettingsFile: path, piModelsFile: path })
      .strict(),
    linux: z
      .object({
        imageReference: z.string().min(1).max(1024).refine(v => !/(^|[:/])latest$/i.test(v)),
        sourceRoot: path,
        piRoot: path,
        node: path,
        daemon: path,
        helper: path,
        probe: path,
        pi: path,
        tmux: path,
        shell: path,
        home: path,
        database: path,
        databases: z.array(path).min(1).max(16),
        stateRoot: path,
        temp: path,
        guard: path,
        environment: path,
        identity: path,
        seats: z
          .array(z.string().regex(/^[a-zA-Z0-9_@.-]{1,80}$/))
          .min(1)
          .max(4),
        piVersion: z.literal("1.0.4"),
        upload: z
          .object({
            origin: z.string().url(),
            privateIPv4: z.string(),
            caFile: path,
            routePrefix: z.literal("/api/elastic-checkpoints/"),
            maxBytes: z
              .number()
              .int()
              .positive()
              .max(600 * 1024 * 1024),
          })
          .strict(),
      })
      .strict(),
  })
  .strict();
export type LinuxWorkerManifest = z.infer<typeof linuxManifestSchema>;
export interface TrustedInstance {
  version: 1;
  imageReference: string;
  resourceId: string;
  vmId: string;
  manifestDigest: string;
  fleetEpoch: string;
  operationId: string;
  privateIPv4: string;
}
function reject(): never {
  throw Error("worker_bootstrap_rejected");
}
export function privateIPv4(ip: string): boolean {
  const parts = ip.split(".").map(Number);
  return (
    parts.length === 4 &&
    parts.every(
      (n, i) =>
        Number.isInteger(n) &&
        n >= 0 &&
        n <= 255 &&
        String(n) === ip.split(".")[i],
    ) &&
    (parts[0] === 10 ||
      (parts[0] === 172 && parts[1]! >= 16 && parts[1]! <= 31) ||
      (parts[0] === 192 && parts[1] === 168))
  );
}
export function parseLinuxManifest(raw: string): LinuxWorkerManifest {
  const m = linuxManifestSchema.parse(JSON.parse(raw));
  const inside = (p: string) =>
    m.ownedRoots.some((r) => p === r || p.startsWith(r + "/"));
  const u = new URL(m.linux.upload.origin);
  if (
    u.protocol !== "https:" ||
    u.hostname !== m.linux.upload.privateIPv4 ||
    u.username ||
    u.password ||
    u.pathname !== "/" ||
    u.search ||
    u.hash ||
    !privateIPv4(m.linux.upload.privateIPv4)
  )
    reject();
  if (
    ![
      m.linux.home,
      m.linux.database,
      m.linux.stateRoot,
      m.linux.temp,
      ...m.linux.databases,
    ].every(inside) ||
    inside(m.controlDirectory) ||
    inside(m.linux.guard) ||
    inside(m.linux.environment) ||
    inside(m.linux.identity) ||
    m.ownedRoots.some((r, i) =>
      m.ownedRoots.some(
        (other, j) => i !== j && (r === other || r.startsWith(other + "/")),
      ),
    )
  )
    reject();
  for (const root of [m.linux.sourceRoot, m.linux.piRoot]) {
    if (inside(root) || !m.pins.some(pin => pin.path === root && pin.kind === 'tree')) reject();
  }
  if (![m.linux.daemon,m.linux.helper,m.linux.probe].every(p => p.startsWith(m.linux.sourceRoot + '/')) || !m.linux.pi.startsWith(m.linux.piRoot + '/')) reject();
  for (const p of [
    m.linux.node,
    m.linux.daemon,
    m.linux.helper,
    m.linux.probe,
    m.linux.pi,
    m.linux.tmux,
    m.linux.shell,
    m.linux.upload.caFile,
  ])
    if (
      inside(p) ||
      !m.pins.some(
        (pin) =>
          pin.path === p ||
          (pin.kind === "tree" && p.startsWith(pin.path + "/")),
      )
    )
      reject();
  if (
    !m.linux.databases.includes(m.linux.database) ||
    m.linux.stateRoot !== join(m.linux.home, "state/pi")
  )
    reject();
  return m;
}
export function assertRootPath(path: string, privateFile = false): void {
  if (resolve(path) !== path) reject();
  for (let p = path; ; p = dirname(p)) {
    const s = lstatSync(p);
    if (
      s.isSymbolicLink() ||
      s.uid !== 0 ||
      (s.mode & 0o022) !== 0 ||
      realpathSync(p) !== p
    )
      reject();
    if (
      p === path &&
      privateFile &&
      (!s.isFile() || s.nlink !== 1 || (s.mode & 0o077) !== 0)
    )
      reject();
    if (p === "/") break;
  }
}
export function readPrivateInput(path: string): unknown {
  assertRootPath(path, true);
  const raw = noFollowRead(path, 128 * 1024).toString().trim();
  const value: unknown = JSON.parse(raw);
  if (JSON.stringify(value) !== raw) reject();
  return value;
}
export function pinDigest(path: string, kind: "file" | "tree"): string {
  assertRootPath(path);
  if (kind === "file") return sha256(noFollowRead(path, 512 * 1024 * 1024));
  const rows: unknown[] = [];
  let n = 0;
  const walk = (p: string) => {
    if (++n > 150000) reject();
    assertRootPath(p);
    const s = lstatSync(p);
    if (s.isDirectory()) {
      rows.push({
        path: p.slice(path.length),
        mode: s.mode & 0o777,
        type: "directory",
      });
      for (const name of readdirSync(p).sort()) walk(join(p, name));
    } else {
      rows.push({
        path: p.slice(path.length),
        mode: s.mode & 0o777,
        sha256: sha256(noFollowRead(p, 512 * 1024 * 1024)),
      });
    }
  };
  walk(path);
  return sha256(JSON.stringify(rows));
}
const ENV = {
  PATH: "/usr/sbin:/usr/bin:/sbin:/bin",
  LANG: "C",
  LC_ALL: "C",
  HOME: "/root",
};
export function linuxCommand(command: string, args: string[]): string {
  return execFileSync(command, args, {
    encoding: "utf8",
    env: ENV,
    timeout: 30000,
    maxBuffer: 16 * 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
}
export function renderWorkerUnit(m: LinuxWorkerManifest): string {
  return `[Unit]\nDescription=OpenRig confined elastic worker\nAfter=network-online.target\n[Service]\nType=exec\nStandardOutput=journal\nStandardError=journal\nUser=${m.uid}\nGroup=${m.gid}\nEnvironmentFile=${m.linux.environment}\nExecCondition=${m.linux.node} ${m.linux.helper} start-condition\nExecStart=${m.linux.node} ${m.linux.daemon}\nWorkingDirectory=${m.linux.home}\nKillMode=control-group\nTimeoutStopSec=45\nRestart=no\nNoNewPrivileges=yes\nCapabilityBoundingSet=\nAmbientCapabilities=\nProtectSystem=strict\nProtectHome=yes\nProtectKernelTunables=yes\nProtectKernelModules=yes\nProtectKernelLogs=yes\nProtectControlGroups=yes\nProtectClock=yes\nProtectHostname=yes\nRestrictSUIDSGID=yes\nRestrictNamespaces=yes\nRestrictRealtime=yes\nLockPersonality=yes\nPrivateDevices=yes\nPrivateMounts=yes\nDelegate=no\nRemoveIPC=yes\nRestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\nSystemCallFilter=~@mount @reboot @swap @privileged\nReadWritePaths=${m.ownedRoots.join(" ")}\nBindPaths=${m.linux.temp}:/tmp ${m.linux.temp}:/var/tmp\nInaccessiblePaths=/dev/shm /run/user /run/dbus /run/systemd/private\nUMask=0077\nTasksMax=256\n[Install]\nWantedBy=multi-user.target\n`;
}
export class LinuxPrivilegedWorkerBoundary implements PrivilegedWorkerBoundary {
  constructor(
    readonly manifest: LinuxWorkerManifest,
    readonly manifestPath: string,
    readonly manifestDigest: string,
  ) {}
  instance(): TrustedInstance {
    assertRootPath(this.manifest.linux.identity, true);
    const x = JSON.parse(
      noFollowRead(this.manifest.linux.identity, 4096).toString(),
    ) as TrustedInstance;
    const m = this.manifest;
    if (
      x.version !== 1 ||
      x.resourceId !== m.resourceId ||
      x.imageReference !== m.linux.imageReference ||
      x.manifestDigest !== this.manifestDigest ||
      !x.vmId ||
      !x.fleetEpoch ||
      !x.operationId ||
      !privateIPv4(x.privateIPv4) ||
      !Object.values(networkInterfaces())
        .flat()
        .some((n) => n?.address === x.privateIPv4 && !n.internal)
    )
      reject();
    return x;
  }
  assertTrustedEnvironment(): void {
    const m = this.manifest;
    if (process.platform !== "linux" || process.geteuid?.() !== 0) reject();
    assertRootPath(this.manifestPath);
    if (sha256(noFollowRead(this.manifestPath, 65536)) !== this.manifestDigest)
      reject();
    assertRootPath(m.controlDirectory);
    if ((lstatSync(m.controlDirectory).mode & 0o077) !== 0) reject();
    for (const pin of m.pins)
      if (pinDigest(pin.path, pin.kind) !== pin.sha256) reject();
    this.instance();
    if (JSON.parse(noFollowRead(join(m.linux.piRoot, "package.json"), 65536).toString()).version !== m.linux.piVersion) reject();
    this.assertAccount();
    const group = `/system.slice/${m.unit}`;
    if (readFileSync("/proc/self/cgroup", "utf8").includes(group)) reject();
    const unit = `/etc/systemd/system/${m.unit}`;
    if (existsSync(unit)) {
      assertRootPath(unit);
      if (readFileSync(unit, "utf8") !== renderWorkerUnit(m)) reject();
      if (
        linuxCommand("/usr/bin/systemctl", [
          "show",
          m.unit,
          "--property=FragmentPath",
          "--value",
        ]).trim() !== unit ||
        linuxCommand("/usr/bin/systemctl", [
          "show",
          m.unit,
          "--property=DropInPaths",
          "--value",
        ]).trim()
      )
        reject();
    }
  }

  private assertAccount(): void {
    const m = this.manifest;
    const rows = readFileSync("/etc/passwd", "utf8")
      .split("\n")
      .map((r) => r.split(":"));
    const accounts = rows.filter(
      (r) => r[0] === m.workerUser || Number(r[2]) === m.uid,
    );
    if (accounts.length === 0) return;
    if (accounts.length !== 1) reject();
    const a = accounts[0]!;
    if (
      a[0] !== m.workerUser ||
      Number(a[2]) !== m.uid ||
      Number(a[3]) !== m.gid ||
      a[5] !== m.linux.home ||
      a[6] !== "/usr/sbin/nologin"
    )
      reject();
    const password = readFileSync("/etc/shadow", "utf8")
      .split("\n")
      .find((r) => r.startsWith(m.workerUser + ":"))
      ?.split(":")[1];
    if (
      !password ||
      !/^(!|\*)/.test(password) ||
      linuxCommand("/usr/bin/id", ["-G", m.workerUser]).trim() !== String(m.gid)
    )
      reject();
    for (const p of [
      `/var/lib/systemd/linger/${m.workerUser}`,
      `/var/spool/cron/crontabs/${m.workerUser}`,
      `/var/spool/cron/${m.workerUser}`,
    ])
      if (existsSync(p)) reject();
    if (existsSync("/usr/bin/sudo")) {
      let permitted = false;
      try {
        linuxCommand("/usr/bin/sudo", ["-n", "-l", "-U", m.workerUser]);
        permitted = true;
      } catch (error) {
        if ((error as { status?: number }).status !== 1) reject();
      }
      if (permitted) reject();
    }
    const mountRows = readFileSync("/proc/self/mountinfo", "utf8")
      .trim()
      .split("\n")
      .map((line) => line.split(" "));
    const mounts = new Set<string>();
    for (const row of mountRows) {
      const split = row.indexOf("-");
      const type = row[split + 1];
      if (["ext4", "xfs", "btrfs", "overlay", "vfat"].includes(type ?? "")) {
        const mount = row[4]!;
        if (mount.includes("\\")) reject();
        mounts.add(mount);
      }
    }
    if (!mounts.has("/")) reject();
    for (const mount of mounts) {
      const files = linuxCommand("/usr/bin/find", [
        mount,
        "-xdev",
        "-uid",
        String(m.uid),
        "-print0",
      ])
        .split("\0")
        .filter(Boolean);
      for (const file of files)
        if (
          !m.ownedRoots.some(
            (root) => file === root || file.startsWith(root + "/"),
          )
        )
          reject();
    }
    for (const dir of ["/etc/systemd/system", "/usr/lib/systemd/system"]) {
      if (!existsSync(dir)) continue;
      for (const name of readdirSync(dir)) {
        const p = join(dir, name);
        if (
          name === m.unit ||
          !name.endsWith(".service") ||
          !lstatSync(p).isFile()
        )
          continue;
        const unit = readFileSync(p, "utf8");
        if (new RegExp(`^User=(?:${m.workerUser}|${m.uid})$`, "m").test(unit))
          reject();
      }
    }
    const userState = linuxCommand("/usr/bin/systemctl", [
      "show",
      `user@${m.uid}.service`,
      "--property=MainPID",
      "--value",
    ]).trim();
    if (userState && userState !== "0") reject();
  }
  configureWorker(material: WorkerRuntimeMaterial, bearer: string): void {
    const m = this.manifest;
    // Root material installation never races an already running dedicated UID.
    for (const pid of readdirSync('/proc').filter(p => /^\d+$/.test(p))) {
      const status = readFileSync(`/proc/${pid}/status`, 'utf8');
      if (Number(/^Uid:\s+(\d+)/m.exec(status)?.[1]) === m.uid) reject();
    }
    const ensureDirectory = (path: string): void => {
      const parent = dirname(path);
      if (!existsSync(parent)) ensureDirectory(parent);
      if (realpathSync(parent) !== parent || !lstatSync(parent).isDirectory()) reject();
      if (!existsSync(path)) mkdirSync(path, { mode: 0o700 });
      if (realpathSync(path) !== path || !lstatSync(path).isDirectory()) reject();
      for (let owned = path; m.ownedRoots.some(root => owned === root || owned.startsWith(root + '/')); owned = dirname(owned)) {
        if (realpathSync(owned) !== owned || !lstatSync(owned).isDirectory()) reject();
        chownSync(owned, m.uid, m.gid); chmodSync(owned, 0o700);
      }
    };
    const passwd = readFileSync("/etc/passwd", "utf8")
      .split("\n")
      .filter(Boolean)
      .map((x) => x.split(":"));
    let account = passwd.find(
      (p) => Number(p[2]) === m.uid || p[0] === m.workerUser,
    );
    if (!account) {
      linuxCommand("/usr/sbin/groupadd", [
        "--gid",
        String(m.gid),
        m.workerUser,
      ]);
      linuxCommand("/usr/sbin/useradd", [
        "--uid",
        String(m.uid),
        "--gid",
        String(m.gid),
        "--home-dir",
        m.linux.home,
        "--shell",
        "/usr/sbin/nologin",
        "--no-create-home",
        "--no-log-init",
        m.workerUser,
      ]);
      account = readFileSync("/etc/passwd", "utf8")
        .split("\n")
        .map((x) => x.split(":"))
        .find((p) => p[0] === m.workerUser);
    }
    if (
      !account ||
      account[0] !== m.workerUser ||
      Number(account[2]) !== m.uid ||
      Number(account[3]) !== m.gid ||
      account[5] !== m.linux.home ||
      account[6] !== "/usr/sbin/nologin"
    )
      reject();
    const shadow = readFileSync("/etc/shadow", "utf8")
      .split("\n")
      .find((l) => l.startsWith(m.workerUser + ":"))
      ?.split(":")[1];
    if (!shadow || !/^(!|\*)/.test(shadow)) reject();
    if (
      linuxCommand("/usr/bin/id", ["-G", m.workerUser]).trim() !== String(m.gid)
    )
      reject();
    for (const p of [
      `/var/lib/systemd/linger/${m.workerUser}`,
      `/var/spool/cron/crontabs/${m.workerUser}`,
      `/var/spool/cron/${m.workerUser}`,
    ])
      if (existsSync(p)) reject();
    for (const p of m.linux.databases) {
      if (existsSync(p)) {
        if (realpathSync(p) !== p || !lstatSync(p).isFile()) reject();
        chownSync(p, m.uid, m.gid);
        chmodSync(p, 0o600);
        for (const suffix of ["-wal", "-shm"])
          if (existsSync(p + suffix)) {
            if (realpathSync(p + suffix) !== p + suffix) reject();
            chownSync(p + suffix, m.uid, m.gid);
          }
      }
    }
    if (
      existsSync(m.linux.guard) &&
      JSON.parse(noFollowRead(m.linux.guard, 4096).toString()).phase ===
        "sealed"
    )
      reject();
    for (const p of [
      ...m.ownedRoots,
      m.linux.home,
      m.linux.stateRoot,
      m.linux.temp,
    ]) {
      ensureDirectory(p);
      if (realpathSync(p) !== p) reject();
      chownSync(p, m.uid, m.gid);
      chmodSync(p, 0o700);
    }
    for (const seat of m.linux.seats) {
      const paths = piSeatPaths(m.linux.stateRoot, seat);
      for (const p of [paths.seatRoot, paths.agentDir, paths.sessionsDir]) {
        ensureDirectory(p);
        if (realpathSync(p) !== p) reject();
        chownSync(p, m.uid, m.gid);
      }
      for (const [name, value] of [
        ["auth", material.piAuth],
        ["settings", material.piSettings],
        ["models", material.piModels],
      ] as const) {
        const p = join(paths.agentDir, `${name}.json`);
        if (existsSync(p)) {
          if (noFollowRead(p).toString() !== JSON.stringify(value)) reject();
        } else {
          durableWrite(p, JSON.stringify(value));
          chownSync(p, m.uid, m.gid);
        }
      }
    }
    const x = this.instance();
    const identity = {
      version: 1,
      phase: "ready",
      workerId: azureWorkerId(m.resourceId, x.vmId),
      vmId: x.vmId,
      manifestDigest: this.manifestDigest,
    };
    durableWrite(m.linux.guard, JSON.stringify(identity), 0o644);
    // Fixed root-owned record; the dedicated group can read it, the worker cannot replace it.
    const gitConfig = join(m.linux.home, '.gitconfig');
    const gitConfigBytes = '[credential "https://github.com"]\n\thelper = !/opt/node/bin/gh auth git-credential\n[credential]\n\tinteractive = false\n[core]\n\thooksPath = /dev/null\n';
    if (existsSync(gitConfig)) { if (noFollowRead(gitConfig, 16384).toString() !== gitConfigBytes) reject(); }
    else { durableWrite(gitConfig, gitConfigBytes); chownSync(gitConfig, m.uid, m.gid); }
    const ghConfigDirectory = '/etc/openrig-elastic/gh-empty';
    if (!existsSync(ghConfigDirectory)) mkdirSync(ghConfigDirectory, { mode: 0o755 });
    assertRootPath(ghConfigDirectory);
    if (!lstatSync(ghConfigDirectory).isDirectory() || readdirSync(ghConfigDirectory).length !== 0) reject();
    const localApiPath = "/etc/openrig-elastic/local-api.json";
    durableWrite(localApiPath, JSON.stringify({ version: 1, workerId: identity.workerId, vmId: x.vmId,
      manifestDigest: this.manifestDigest, baseUrl: `http://${x.privateIPv4}:${m.port}`, bearer }), 0o640);
    chownSync(localApiPath, 0, m.gid);

    const env = {
      HOME: m.linux.home,
      SHELL: m.linux.shell,
      OPENRIG_HOME: m.linux.home,
      OPENRIG_DB: m.linux.database,
      OPENRIG_BIND_HOST: x.privateIPv4,
      OPENRIG_URL: `http://${x.privateIPv4}:${m.port}`,
      OPENRIG_PORT: String(m.port),
      OPENRIG_AUTH_BEARER_TOKEN: bearer,
      OPENRIG_ELASTIC_GUARD: m.linux.guard,
      OPENRIG_ELASTIC_PI: m.linux.pi,
      OPENRIG_ELASTIC_MANIFEST: this.manifestPath,
      OPENRIG_ELASTIC_MANIFEST_DIGEST: this.manifestDigest,
      TMPDIR: m.linux.temp,
      TMUX_TMPDIR: m.linux.temp,
      XDG_CACHE_HOME: join(m.linux.home, "cache"),
      PATH: `${dirname(m.linux.pi)}:${dirname(m.linux.node)}:/usr/bin:/bin`,
    };
    if (Object.values(env).some((v) => /[\r\n"\\]/.test(v))) reject();
    durableWrite(
      m.linux.environment,
      Object.entries(env)
        .map(([k, v]) => `${k}="${v}"`)
        .join("\n") + "\n",
    );
    durableWrite(`/etc/systemd/system/${m.unit}`, renderWorkerUnit(m), 0o644);
    linuxCommand("/usr/bin/systemd-analyze", [
      "verify",
      `/etc/systemd/system/${m.unit}`,
    ]);
    linuxCommand("/usr/bin/systemctl", ["daemon-reload"]);
  }
  startUnit(): void {
    this.assertTrustedEnvironment();
    if (
      JSON.parse(noFollowRead(this.manifest.linux.guard, 4096).toString())
        .phase !== "ready"
    )
      reject();
    linuxCommand("/usr/bin/systemctl", ["start", this.manifest.unit]);
  }
  publishAdmission(state:ElasticWorkerState,purpose:'removal'|'continuation'|'transition',operationId:string):void {
    durableWrite(this.manifest.linux.guard,JSON.stringify({version:1,phase:purpose==='transition'?'transition':state.phase,
      workerId:state.workerId,vmId:state.vmId,manifestDigest:state.manifestDigest,generation:state.generation,purpose,operationId}),0o644);
  }
  installSealMarker(): void {
    const m = this.manifest;
    const x = this.instance();
    durableWrite(
      m.linux.guard,
      JSON.stringify({
        version: 1,
        phase: "sealed",
        workerId: azureWorkerId(m.resourceId, x.vmId),
        vmId: x.vmId,
        manifestDigest: this.manifestDigest,
      }),
      0o644,
    );
  }
  stopUnit(): void {
    linuxCommand("/usr/bin/systemctl", ["stop", this.manifest.unit]);
  }
  processes(): WorkerProcessProof {
    return inspectLinuxProcesses(this.manifest);
  }
}
export interface LinuxProcessIdentity {
  pid: number;
  startTime: string;
  uid: number;
  ppid: number;
  state: string;
  cgroup: string;
  executable: string;
  argv: string[];
}
export function procIdentity(pid: number): LinuxProcessIdentity {
  const base = `/proc/${pid}`;
  const status = readFileSync(join(base, "status"), "utf8");
  const uid = Number(/^Uid:\s+(\d+)/m.exec(status)?.[1]);
  const raw = readFileSync(join(base, "stat"), "utf8");
  const fields = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
  const groups = readFileSync(join(base, "cgroup"), "utf8").trim().split("\n");
  if (groups.length !== 1 || !groups[0]?.startsWith("0::")) reject();
  return {
    pid,
    startTime: fields[19]!,
    uid,
    ppid: Number(fields[1]),
    state: fields[0]!,
    cgroup: groups[0].slice(3),
    executable: readlinkSync(join(base, "exe")),
    argv: readFileSync(join(base, "cmdline"), "utf8")
      .split("\0")
      .filter(Boolean),
  };
}
export function inspectLinuxProcesses(
  m: LinuxWorkerManifest,
): WorkerProcessProof {
  const expected = `/system.slice/${m.unit}`;
  const result: WorkerProcessProof = {
    observedAt: Date.now(),
    complete: false,
    idle: false,
    processes: 0,
    uid: m.uid,
    cgroup: expected,
  };
  try {
    const privileged = process.geteuid?.() === 0;
    const cg = privileged
      ? linuxCommand("/usr/bin/systemctl", [
          "show",
          m.unit,
          "--property=ControlGroup",
          "--value",
        ]).trim()
      : readFileSync("/proc/self/cgroup", "utf8").trim().replace(/^0::/, "");
    const main = privileged
      ? Number(
          linuxCommand("/usr/bin/systemctl", [
            "show",
            m.unit,
            "--property=MainPID",
            "--value",
          ]).trim(),
        )
      : process.pid;
    if (cg && cg !== expected) reject();
    const ids: LinuxProcessIdentity[] = [];
    for (const name of readdirSync("/proc")) {
      if (!/^\d+$/.test(name)) continue;
      const status = readFileSync(`/proc/${name}/status`, "utf8");
      const uid = Number(/^Uid:\s+(\d+)/m.exec(status)?.[1]);
      const group = readFileSync(`/proc/${name}/cgroup`, "utf8");
      if (
        uid === m.uid ||
        group
          .split("\n")
          .some(
            (g) => g === `0::${expected}` || g.startsWith(`0::${expected}/`),
          )
      ) {
        const p = procIdentity(Number(name));
        if (p.uid !== m.uid || p.cgroup !== expected) reject();
        ids.push(p);
      }
    }
    result.processes = ids.length;
    if (ids.length === 0) {
      if (main !== 0 || cg) reject();
      result.complete = true;
      result.idle = true;
      return result;
    }
    if (
      !ids.some(
        (p) =>
          p.pid === main &&
          p.executable === m.linux.node &&
          p.argv[1] === m.linux.daemon,
      )
    )
      reject();
    const known = new Set<number>([main]);
    for (const seat of m.linux.seats) {
      const file = piSeatPaths(m.linux.stateRoot, seat).runnerStatePath;
      if (!existsSync(file)) continue;
      const state = JSON.parse(noFollowRead(file, 65536).toString()) as {
        launchId?: string;
        ready?: boolean;
        exited?: unknown;
        idleProof?: {
          at: number;
          runnerPid: number;
          childPid: number;
          runnerStartTime: string;
          childStartTime: string;
          launchId: string;
          idle: boolean;
        };
      };
      if (state.exited && state.ready === false) continue;
      const p = state.idleProof;
      if (
        !p ||
        p.launchId !== state.launchId ||
        !p.idle ||
        Date.now() - p.at > 3000 ||
        p.at > Date.now()
      )
        reject();
      const runner = ids.find((i) => i.pid === p.runnerPid);
      const child = ids.find((i) => i.pid === p.childPid);
      if (
        !runner ||
        !child ||
        runner.executable !== m.linux.node ||
        child.ppid !== runner.pid ||
        runner.startTime !== p.runnerStartTime || child.startTime !== p.childStartTime ||
        child.executable !== m.linux.node ||
        child.argv[1] !== m.linux.pi ||
        runner.argv[1] !== join(dirname(m.linux.helper), "pi-runner.js") ||
        !runner.argv.includes("--launch-id") ||
        runner.argv[runner.argv.indexOf("--launch-id") + 1] !== p.launchId
      )
        reject();
      known.add(runner.pid);
      known.add(child.pid);
    }
    for (const p of ids) {
      if (known.has(p.pid)) continue;
      if (p.executable !== m.linux.tmux && !(p.executable === m.linux.shell && p.state === 'S' && ids.some(child => known.has(child.pid) && child.ppid === p.pid))) reject();
      known.add(p.pid);
    }
    for (const p of ids)
      if (JSON.stringify(procIdentity(p.pid)) !== JSON.stringify(p)) reject();
    result.complete = true;
    result.idle = true;
  } catch {
    /* Any missing/racing/unknown evidence retains the worker. */
  }
  return result;
}
