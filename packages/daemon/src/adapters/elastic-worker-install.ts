import { existsSync, mkdirSync, chmodSync } from "node:fs";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { migrate } from "../db/migrate.js";
import { ALL_MIGRATIONS } from "../db/all-migrations.js";
import {
  assertRootPath,
  parseLinuxManifest,
  pinDigest,
} from "./linux-privileged-worker-boundary.js";
import {
  durableWrite,
  noFollowRead,
  sha256,
} from "./elastic-worker-checkpoint.js";
/** Image installation only. All source trees, CA, manifest and ARM identity must already be staged by the trusted image/controller. */
export function installElasticWorker(): void {
  if (
    process.platform !== "linux" ||
    process.geteuid?.() !== 0 ||
    process.argv.length !== 2
  )
    throw Error();
  const path = "/etc/openrig-elastic/manifest.json";
  assertRootPath(path);
  assertRootPath("/etc/openrig-elastic/manifest.sha256");
  const raw = noFollowRead(path, 65536);
  if (
    noFollowRead("/etc/openrig-elastic/manifest.sha256", 128)
      .toString()
      .trim() !== sha256(raw)
  )
    throw Error();
  const m = parseLinuxManifest(raw.toString());
  for (const pin of m.pins)
    if (pinDigest(pin.path, pin.kind) !== pin.sha256) throw Error();
  for (const p of [m.controlDirectory, ...m.ownedRoots]) {
    assertRootPath(dirname(p));
    if (!existsSync(p)) mkdirSync(p, { mode: 0o700 });
    assertRootPath(p);
  }
  chmodSync(m.controlDirectory, 0o700);
  for (const p of [
    m.linux.home,
    dirname(m.linux.database),
    m.linux.stateRoot,
    m.linux.temp,
  ]) {
    mkdirSync(p, { recursive: true, mode: 0o700 });
    assertRootPath(p);
  }
  if (existsSync(m.linux.database)) throw Error();
  const db = new Database(m.linux.database);
  try {
    migrate(db, ALL_MIGRATIONS);
  } finally {
    db.close();
  }
  chmodSync(m.linux.database, 0o600);
  if (!existsSync(m.linux.guard))
    durableWrite(
      m.linux.guard,
      JSON.stringify({ version: 1, phase: "pending" }),
      0o644,
    );
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  try {
    installElasticWorker();
    process.stdout.write("worker_image_installed\n");
  } catch {
    process.stderr.write("worker_image_install_rejected\n");
    process.exitCode = 1;
  }
}
