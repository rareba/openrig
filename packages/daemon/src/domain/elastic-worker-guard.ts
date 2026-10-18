import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync, realpathSync } from "node:fs";
import Database from "better-sqlite3";
/** Explicit opt-in only. This is a final-effect fence, never a removal certificate. */
export function assertElasticEffectAllowed(db?: Database.Database, options: { allowDraining?: boolean } = {}): void {
  const path = process.env.OPENRIG_ELASTIC_GUARD;
  if (path === undefined) {
    if (existsSync('/etc/openrig-elastic/manifest.json')) throw Error('elastic_worker_effect_denied');
    return;
  }
  try {
    const st = lstatSync(path);
    if (
      realpathSync(path) !== path ||
      !st.isFile() ||
      st.uid !== 0 ||
      (st.mode & 0o022) !== 0 ||
      st.size > 4096
    )
      throw Error();
    const v = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
    if (
      v.version !== 1 ||
      (v.phase !== "ready" && v.phase !== "draining") ||
      typeof v.workerId !== "string" ||
      typeof v.vmId !== "string" ||
      typeof v.manifestDigest !== "string"
    )
      throw Error();
    const ownedReader = db ? null : new Database(process.env.OPENRIG_DB ?? '', {readonly:true,fileMustExist:true});
    try {
      const state = (db ?? ownedReader!)
        .prepare(
          "SELECT worker_id,vm_id,manifest_digest,phase FROM elastic_worker_control WHERE id=1",
        )
        .get() as Record<string, unknown> | undefined;
      if (
        !state ||
        state.phase === "sealed" ||
        (state.phase === "draining" && !options.allowDraining) ||
        state.worker_id !== v.workerId ||
        state.vm_id !== v.vmId ||
        state.manifest_digest !== v.manifestDigest
      )
        throw Error();
    } finally { ownedReader?.close(); }
  } catch {
    throw Error("elastic_worker_effect_denied");
  }
}

export function assertElasticSeatAllowed(
  sessionName: string,
  runtime: string | null | undefined,
  stateRoot?: string,
): void {
  assertElasticEffectAllowed();
  if (process.env.OPENRIG_ELASTIC_GUARD === undefined) return;
  try {
    const path = process.env.OPENRIG_ELASTIC_MANIFEST!;
    const st = lstatSync(path);
    if (
      !st.isFile() ||
      st.uid !== 0 ||
      st.mode & 0o022 ||
      realpathSync(path) !== path ||
      st.size > 65536
    )
      throw Error();
    const bytes = readFileSync(path);
    if (
      createHash("sha256").update(bytes).digest("hex") !==
      process.env.OPENRIG_ELASTIC_MANIFEST_DIGEST
    )
      throw Error();
    const m = JSON.parse(bytes.toString());
    if (
      runtime !== "pi" ||
      !m.linux.seats.includes(sessionName) ||
      (stateRoot !== undefined && stateRoot !== m.linux.stateRoot)
    )
      throw Error();
  } catch {
    throw Error("elastic_worker_profile_denied");
  }
}

/** Repeated at the actual spawn/transport boundary after async adapter work. */
export function assertElasticTmuxEffectAllowed(argv: readonly string[]): void {
  if (["new-session", "new-window", "split-window", "respawn-pane", "send-keys", "paste-buffer", "load-buffer", "source-file", "run-shell"].some(command => argv.includes(command))) assertElasticEffectAllowed();
}
