import { pathToFileURL } from "node:url";
import { readFileSync, lstatSync, realpathSync } from "node:fs";
/** Private fixed helper. Limits apply before Node starts; verify them before loading native SQLite. */
async function inspect(): Promise<void> {
    if (process.platform !== "linux" || process.argv.length !== 4)
        throw Error();
    const [path, kind] = process.argv.slice(2);
    if (!path || !['native', 'ordinary'].includes(kind!))
        throw Error();
    const limits = readFileSync("/proc/self/limits", "utf8");
    for (const [label, value] of [["Max cpu time", "2"], ["Max data size", "536870912"], ["Max address space", "2147483648"], ["Max file size", "0"], ["Max core file size", "0"], ["Max open files", "64"]]) {
        const line = limits.split("\n").find(row => row.startsWith(label!));
        if (!line || line.slice(label!.length).trim().split(/\s+/).slice(0, 2).some(v => v !== value))
            throw Error();
    }
    const stat = lstatSync(path);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || stat.uid !== process.getuid?.() || (stat.mode & 0o777) !== 0o600 || stat.size > 64 * 1024 * 1024 || realpathSync(path) !== path)
        throw Error();
    if(process.env.SQLITE_USE_URI!=="1")throw Error();
    const uri=pathToFileURL(path);uri.searchParams.set("mode","ro");uri.searchParams.set("immutable","1");
    const { inspectCheckpointSqlite } = await import("./elastic-checkpoint-sqlite.js");
    const inspection = inspectCheckpointSqlite(uri.href);
    let native = null;
    if (kind === "native") {
        const { default: Database } = await import("better-sqlite3");
        const { ElasticWorkerControl } = await import("../domain/elastic-worker-control.js");
        const db = new Database(uri.href, { readonly: true, fileMustExist: true });
        try {
            db.pragma("trusted_schema=OFF");
            db.pragma("query_only=ON");
            db.pragma("temp_store=MEMORY");
            const control = new ElasticWorkerControl(db);
            native = { state: control.read(), ledger: control.ledger() };
        }
        finally {
            db.close();
        }
    }
    const output = JSON.stringify({ inspection, native });
    if (Buffer.byteLength(output) > 262144)
        throw Error();
    process.stdout.write(output);
}
void inspect().catch(() => { process.stderr.write("checkpoint_sqlite_rejected\n"); process.exitCode = 1; });
