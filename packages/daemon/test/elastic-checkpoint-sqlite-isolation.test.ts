import { afterEach, describe, expect, it, vi } from "vitest";
const execution = vi.hoisted(() => ({ signal: "", code: "" }));
vi.mock("node:child_process", async (original) => {
    const cp = await original<typeof import("node:child_process")>();
    return { ...cp, execFileSync: (...args: Parameters<typeof cp.execFileSync>) => { try {
            return cp.execFileSync(...args);
        }
        catch (error) {
            if (args[0] === "/usr/bin/prlimit" && typeof error === "object" && error !== null) {
                execution.signal = "signal" in error ? String(error.signal) : "";
                execution.code = "code" in error ? String(error.code) : "";
            }
            throw error;
        } } };
});
import { execFileSync } from "node:child_process";
import { mkdtempSync, realpathSync, chmodSync, rmSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { inspectArchivedSqlite, requireArchivedSqlitePlatform } from "../src/adapters/elastic-checkpoint-sqlite-isolation.js";
import { inspectCheckpointSqlite } from "../src/adapters/elastic-checkpoint-sqlite.js";
const dirs: string[] = [];
function fixture(sql: string, rows = 1) { const root = realpathSync(mkdtempSync(join(tmpdir(), "sqlite-limits-"))); dirs.push(root); const path = join(root, "archive.sqlite"), db = new Database(path); db.pragma("ignore_check_constraints=ON"); db.exec(sql); if (rows)
    db.exec(`WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<${rows}) INSERT INTO t SELECT v FROM n`); db.close(); chmodSync(path, 0o600); return path; }
function generated(path: string, expression: string) { const db = new Database(path); db.unsafeMode(true); db.pragma("writable_schema=ON"); db.prepare("UPDATE sqlite_schema SET sql=? WHERE name='t'").run(`CREATE TABLE t(v,g TEXT GENERATED ALWAYS AS (${expression}) VIRTUAL)`); db.close(); }
afterEach(() => { for (const p of dirs.splice(0))
    rmSync(p, { recursive: true, force: true }); execution.signal = ""; execution.code = ""; });
describe("OS-enforced archive SQLite boundary", () => {
    it.runIf(process.platform !== "linux")("fails closed clearly on unsupported platforms", () => { expect(() => requireArchivedSqlitePlatform()).toThrow("checkpoint_sqlite_platform_unsupported"); });
    it.runIf(process.platform === "linux")("preserves ordinary CHECK schemas, lossless TEXT and arbitrary table values inside actual limits", () => {
        const path = fixture("CREATE TABLE t(v CHECK(v>0));CREATE TABLE exact(v);INSERT INTO exact VALUES(CAST(x'80' AS TEXT)),(9007199254740993),(1.5),(x'00FF'),(NULL)");
        expect(inspectArchivedSqlite(path, false).inspection).toEqual(inspectCheckpointSqlite(path));
        const db = new Database(path);
        db.exec("UPDATE exact SET v=CAST(x'81' AS TEXT) WHERE typeof(v)='text'");
        db.close();
        const a = inspectArchivedSqlite(path, false).inspection.logicalDigest;
        expect(a).toBe(inspectCheckpointSqlite(path).logicalDigest);
    });
    it.runIf(process.platform === "linux")("rejects native generated-expression allocation larger than the OS data/address-space budget", () => {
        const path = fixture("CREATE TABLE t(v)");
        generated(path, "printf('%800000000s',v)");
        const before = Date.now();
        expect(() => inspectArchivedSqlite(path, false)).toThrow();
        expect(Date.now() - before).toBeLessThan(6000);
        expect(readFileSync(path).length).toBeLessThan(65536);
    });
    it.runIf(process.platform === "linux")("kills expensive generated-expression evaluation within CPU/wall limits", () => {
        const path = fixture("CREATE TABLE t(v CHECK(v>0))", 200000);
        generated(path, "length(lower(hex(zeroblob(131072+(v%2)))))");
        const before = Date.now();
        expect(() => inspectArchivedSqlite(path, false)).toThrow();
        expect(Date.now() - before).toBeLessThan(6500);
        expect(execution.signal).toBe("SIGKILL");
    }, 10000);
    it.runIf(process.platform === "linux")("refuses helper execution without exact kernel-enforced limits before loading SQLite", () => {
        const path = fixture("CREATE TABLE t(v)");
        const helper = fileURLToPath(new URL("../dist/adapters/elastic-checkpoint-sqlite-helper.js", import.meta.url));
        expect(() => execFileSync(process.execPath, [helper, path, "ordinary"], { stdio: "pipe", timeout: 1000 })).toThrow();
    });
});
