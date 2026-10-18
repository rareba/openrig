import Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { workerDigest } from "../domain/elastic-worker-control.js";
const LIMIT = 512 * 1024 * 1024;
function fail(): never { throw Error("checkpoint_sqlite_rejected"); }
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
/** SQL converts TEXT to bytes before the driver can replace invalid UTF8. */
function value(type: unknown, v: unknown): unknown {
    switch (type) {
        case "null":
            if (v === null)
                return [type];
            break;
        case "integer":
            if (typeof v === "bigint")
                return [type, v.toString()];
            break;
        case "real":
            if (typeof v === "number") {
                const bits = Buffer.alloc(8);
                bits.writeDoubleBE(v);
                return [type, bits.toString("hex")];
            }
            break;
        case "text":
        case "blob":
            if (Buffer.isBuffer(v))
                return [type, v.toString("base64")];
            break;
    }
    return fail();
}
export function inspectCheckpointSqlite(path: string): {
    schemaDigest: string;
    tables: string[];
    logicalDigest: string;
} {
    const db = new Database(path, { readonly: true, fileMustExist: true });
    try {
        db.pragma("trusted_schema=OFF");
        db.pragma("query_only=ON");
        db.pragma("temp_store=MEMORY");
        if (db.pragma("integrity_check", { simple: true }) !== "ok" || (db.pragma("foreign_key_check") as unknown[]).length)
            fail();
        // Invalid UTF8 identifiers/schema cannot safely become SQL identifiers; refuse them explicitly.
        const rawSchema = db.prepare("SELECT CAST(type AS BLOB),CAST(name AS BLOB),CAST(tbl_name AS BLOB),CAST(sql AS BLOB) FROM sqlite_schema ORDER BY type,name").raw(true).all();
        const decoder = new TextDecoder("utf-8", { fatal: true });
        for (const row of rawSchema)
            for (const cell of row as unknown[]) {
                if (cell !== null) {
                    if (!Buffer.isBuffer(cell))
                        fail();
                    decoder.decode(cell);
                }
            }
        const schema = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name").all();
        const tables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").all() as Array<{
            name: string;
        }>;
        const kinds = db.pragma("table_list") as Array<{
            schema: string;
            name: string;
            wr: number;
            type: string;
        }>;
        let bytes = 0, rowCount = 0;
        const deadline = Date.now() + 30000;
        const content = tables.map(t => {
            const kind = kinds.find(k => k.schema === "main" && k.name === t.name);
            if (!kind || kind.type === "virtual")
                fail();
            const columns = db.pragma(`table_xinfo(${quote(t.name)})`) as Array<{
                name: string;
            }>;
            const rowid = ["rowid", "_rowid_", "oid"].find(n => !columns.some(c => c.name.toLowerCase() === n));
            if (!kind.wr && !rowid)
                fail();
            const cols = [...(!kind.wr ? [rowid!] : []), ...columns.map(c => c.name)];
            const select = cols.flatMap(c => [`typeof(${quote(c)})`, `CASE WHEN typeof(${quote(c)})='text' THEN CAST(${quote(c)} AS BLOB) ELSE ${quote(c)} END`]).join(",");
            const rows: string[] = [];
            for (const raw of db.prepare(`SELECT ${select} FROM ${quote(t.name)}`).raw(true).safeIntegers(true).iterate()) {
                if (++rowCount > 1000000 || Date.now() > deadline)
                    fail();
                const row = raw as unknown[];
                const typed: unknown[] = [];
                for (let i = 0; i < row.length; i += 2)
                    typed.push(value(row[i], row[i + 1]));
                const encoded = JSON.stringify(typed);
                bytes += Buffer.byteLength(encoded);
                if (bytes > LIMIT)
                    fail();
                rows.push(encoded);
            }
            return { name: t.name, columns, rows: rows.sort() };
        });
        const metadata = { userVersion: db.pragma("user_version", { simple: true }), applicationId: db.pragma("application_id", { simple: true }), encoding: db.pragma("encoding", { simple: true }) };
        const hash = createHash("sha256").update(JSON.stringify({ schema, content, metadata })).digest("hex");
        return { schemaDigest: workerDigest(schema), tables: tables.map(t => t.name), logicalDigest: hash };
    }
    catch {
        return fail();
    }
    finally {
        db.close();
    }
}
