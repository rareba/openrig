import { inspectManagedTmuxIpc } from "./managed-tmux-ipc.js";
import Database from "better-sqlite3";
import {
  parseLinuxManifest,
  inspectLinuxProcesses,
  assertRootPath,
} from "./linux-privileged-worker-boundary.js";
import { noFollowRead, sha256 } from "./elastic-worker-checkpoint.js";
import { ElasticWorkerControl } from "../domain/elastic-worker-control.js";
import { assertElasticEffectAllowed } from "../domain/elastic-worker-guard.js";
import {
  inspectElasticWorker,
  inspectWorkerFilesystem,
} from "./elastic-worker-probe.js";
import type { ElasticWorkerProbeReader } from "../routes/elastic-worker.js";
import { pathToFileURL } from "node:url";
export function loadForegroundManifest() {
  const path = process.env.OPENRIG_ELASTIC_MANIFEST;
  if (!path) throw Error("elastic_worker_unavailable");
  assertRootPath(path);
  const bytes = noFollowRead(path, 65536);
  if (sha256(bytes) !== process.env.OPENRIG_ELASTIC_MANIFEST_DIGEST)
    throw Error("elastic_worker_unavailable");
  const manifest = parseLinuxManifest(bytes.toString());
  if (process.env.TMUX_TMPDIR !== manifest.linux.temp) throw Error("elastic_worker_unavailable");
  return manifest;
}
export function workerDeclaredCwds(control: ElasticWorkerControl): string[] {
  const rows = control.db.prepare("SELECT cwd FROM nodes").all() as Array<{
    cwd: string | null;
  }>;
  const paths = rows.map((r) => r.cwd ?? "");
  const workspaces = control.db
    .prepare("SELECT workspace_json FROM rigs WHERE workspace_json IS NOT NULL")
    .all() as Array<{ workspace_json: string }>;
  for (const r of workspaces) {
    const v = JSON.parse(r.workspace_json);
    if (typeof v.workspaceRoot !== "string" || !Array.isArray(v.repos))
      throw Error("elastic_worker_unavailable");
    paths.push(
      v.workspaceRoot,
      ...v.repos.map((repo: { path: unknown }) =>
        typeof repo.path === "string" ? repo.path : "",
      ),
    );
    if (v.knowledgeRoot !== undefined)
      paths.push(typeof v.knowledgeRoot === "string" ? v.knowledgeRoot : "");
  }
  return paths;
}
export function createElasticForegroundProbe(
  dbPath: string,
): ElasticWorkerProbeReader | undefined {
  if (process.env.OPENRIG_ELASTIC_GUARD === undefined) return undefined;
  const m = loadForegroundManifest();
  if (
    m.linux.database !== dbPath ||
    m.linux.guard !== process.env.OPENRIG_ELASTIC_GUARD
  )
    throw Error("elastic_worker_unavailable");
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  const control = new ElasticWorkerControl(db);
  assertElasticEffectAllowed(db, { allowDraining: true });
  return () =>
    inspectElasticWorker({
      control,
      ownedRoots: m.ownedRoots,
      declaredCwds: workerDeclaredCwds(control),
      processes: inspectLinuxProcesses(m),
      sqlitePaths: m.linux.databases,
      managedIpc: inspectManagedTmuxIpc(m),
    });
}
// This fixed executable is invoked as the dedicated UID by the root boundary; Git never runs as root.
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  try {
    const m = loadForegroundManifest();
    if (process.getuid?.() !== m.uid) throw Error();
    const db = new Database(m.linux.database, {
      readonly: true,
      fileMustExist: true,
    });
    try {
      process.stdout.write(
        JSON.stringify(
          inspectWorkerFilesystem(
            m.ownedRoots,
            workerDeclaredCwds(new ElasticWorkerControl(db)),
            m.linux.databases,
            inspectManagedTmuxIpc(m),
          ),
        ),
      );
    } finally {
      db.close();
    }
  } catch {
    process.stderr.write("elastic_worker_unavailable\n");
    process.exitCode = 1;
  }
}
