import { execFileSync } from "node:child_process";
import { lstatSync, realpathSync, mkdtempSync, chmodSync, rmSync } from "node:fs";
import Database from "better-sqlite3";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { checkpointFailure as fail } from "./elastic-checkpoint-contract.js";
export const ARCHIVE_SQLITE_LIMITS = { addressSpace: 2147483648, data: 536870912, cpuSeconds: 2, wallMilliseconds: 5000, outputBytes: 262144 } as const;
export const archivedSqliteInspectionSchema = z.object({ schemaDigest: z.string().regex(/^[a-f0-9]{64}$/), tables: z.array(z.string().max(4096)).max(100000), logicalDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict();
const stateSchema = z.object({ version: z.literal(1), workerId: z.string(), vmId: z.string(), manifestDigest: z.string(), phase: z.enum(["ready", "draining", "sealed"]), generation: z.number().int(), drainOperationId: z.string().nullable(), drainStartedAt: z.number().nullable() }).strict();
const ledgerSchema = z.object({ complete: z.boolean(), custody: z.number().int(), runnable: z.number().int(), claims: z.number().int(), unresolvedOutbox: z.number().int(), pendingHumanReturns: z.boolean(), pendingActivationIntents: z.boolean(), pendingTransfers: z.boolean(), clean: z.boolean(), digest: z.string().regex(/^[a-f0-9]{64}$/) }).strict();
export const archivedSqliteResultSchema = z.object({ inspection: archivedSqliteInspectionSchema, native: z.object({ state: stateSchema.nullable(), ledger: ledgerSchema }).strict().nullable() }).strict();
/** The only supported archive SQLite boundary is Linux with kernel-enforced resource limits. */
export function requireArchivedSqlitePlatform(): void { if (process.platform !== "linux")
    throw Error("checkpoint_sqlite_platform_unsupported"); }
export function inspectArchivedSqlite(path: string, native: boolean): z.infer<typeof archivedSqliteResultSchema> {
    requireArchivedSqlitePlatform();
    try {
        const limiter = "/usr/bin/prlimit", node = realpathSync(process.execPath);
        const helper = fileURLToPath(new URL(import.meta.url.endsWith(".ts") ? "../../dist/adapters/elastic-checkpoint-sqlite-helper.js" : "./elastic-checkpoint-sqlite-helper.js", import.meta.url));
        for (const executable of [limiter, node, helper]) {
            const s = lstatSync(executable);
            if (!s.isFile() || s.isSymbolicLink() || (s.mode & 0o022) !== 0 || (s.uid !== 0 && s.uid !== process.getuid?.()))
                fail();
        }
        const limits = ARCHIVE_SQLITE_LIMITS;
        const output = execFileSync(limiter, [`--as=${limits.addressSpace}:${limits.addressSpace}`, `--data=${limits.data}:${limits.data}`, `--cpu=${limits.cpuSeconds}:${limits.cpuSeconds}`, "--fsize=0:0", "--core=0:0", "--nofile=64:64", "--", node, "--jitless", "--max-old-space-size=128", helper, path, native ? "native" : "ordinary"], { env: { PATH: "/usr/bin:/bin", HOME: "/dev/null", XDG_CACHE_HOME: "/dev/null", LANG: "C", SQLITE_USE_URI:"1" }, cwd: "/", encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: limits.wallMilliseconds, killSignal: "SIGKILL", maxBuffer: limits.outputBytes });
        const result = archivedSqliteResultSchema.parse(JSON.parse(output));
        if (native !== (result.native !== null))
            fail();
        return result;
    }
    catch {
        return fail();
    }
}

/** Exercise the same compiled helper, limits and native ABI before activating custody. */
export function preflightArchivedSqlite(directory:string):void {
    requireArchivedSqlitePlatform();
    const root=mkdtempSync(join(directory,'preflight-'));chmodSync(root,0o700);
    try {
        const path=join(root,'probe.sqlite');const db=new Database(path);
        try {db.exec('CREATE TABLE preflight(value INTEGER NOT NULL); INSERT INTO preflight VALUES(1)');}finally{db.close();}
        chmodSync(path,0o600);
        const result=inspectArchivedSqlite(path,false);
        if(!result.inspection.tables.includes('preflight'))fail();
    } finally {rmSync(root,{recursive:true,force:true});}
}
