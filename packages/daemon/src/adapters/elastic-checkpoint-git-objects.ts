import { createHash } from "node:crypto";
import { inflateSync } from "node:zlib";
import { checkpointFailure as fail } from "./elastic-checkpoint-contract.js";
const OBJECT_LIMIT = 64 * 1024 * 1024, TOTAL_LIMIT = 512 * 1024 * 1024, COUNT_LIMIT = 100000;
/** Bound expansion before Git parses untrusted worker objects/pack indexes. */
export function gitObjectBudget() {
    let total = 0, count = 0;
    const account = (size: number) => { if (!Number.isSafeInteger(size) || size < 0 || size > OBJECT_LIMIT || (total += size) > TOTAL_LIMIT || ++count > COUNT_LIMIT)
        fail(); };
    const inflate = (bytes: Buffer) => {
        // info exposes exact compressed bytes consumed, needed to walk pack records without executing Git.
        const result = inflateSync(bytes, { info: true, maxOutputLength: OBJECT_LIMIT }) as unknown as {
            buffer: Buffer;
            engine: {
                bytesWritten: number;
            };
        };
        if (!Buffer.isBuffer(result.buffer) || !Number.isSafeInteger(result.engine.bytesWritten) || result.engine.bytesWritten < 1 || result.engine.bytesWritten > bytes.length)
            fail();
        return result;
    };
    return (path: string, bytes: Buffer, hashLength: number): boolean => {
        if (/^objects\/[a-f0-9]{2}\/[a-f0-9]+$/.test(path)) {
            if (path.split("/").slice(1).join("").length !== hashLength * 2)
                fail();
            const decoded = inflate(bytes);
            if (decoded.engine.bytesWritten !== bytes.length)
                fail();
            const zero = decoded.buffer.indexOf(0);
            if (zero < 0 || zero > 80)
                fail();
            const header = /^(commit|tree|blob|tag) (0|[1-9][0-9]*)$/.exec(decoded.buffer.subarray(0, zero).toString("ascii"));
            if (!header || Number(header[2]) !== decoded.buffer.length - zero - 1)
                fail();
            account(Number(header[2]));
            return true;
        }
        if (/^objects\/pack\/pack-[a-f0-9]+\.pack$/.test(path)) {
            if (bytes.length < 12 + hashLength || bytes.subarray(0, 4).toString() !== "PACK" || ![2, 3].includes(bytes.readUInt32BE(4)) || bytes.readUInt32BE(8) > COUNT_LIMIT)
                fail();
            const end = bytes.length - hashLength;
            if (!createHash(hashLength === 32 ? "sha256" : "sha1").update(bytes.subarray(0, end)).digest().equals(bytes.subarray(end)))
                fail();
            let at = 12;
            for (let i = 0; i < bytes.readUInt32BE(8); i++) {
                if (at >= end)
                    fail();
                let byte = bytes[at++]!, size = byte & 15, shift = 4;
                const type = (byte >> 4) & 7;
                while (byte & 128) {
                    if (at >= end || shift > 49)
                        fail();
                    byte = bytes[at++]!;
                    size += (byte & 127) * 2 ** shift;
                    shift += 7;
                }
                if (size > OBJECT_LIMIT || ![1, 2, 3, 4, 6, 7].includes(type))
                    fail();
                if (type === 6) {
                    let n = 0;
                    do {
                        if (at >= end || ++n > 10)
                            fail();
                        byte = bytes[at++]!;
                    } while (byte & 128);
                }
                if (type === 7)
                    at += hashLength;
                if (at >= end)
                    fail();
                const decoded = inflate(bytes.subarray(at, end));
                at += decoded.engine.bytesWritten;
                if (decoded.buffer.length !== size)
                    fail();
                if (type === 6 || type === 7) {
                    let offset = 0;
                    const variable = () => { let n = 0, power = 0, part = 0; do {
                        if (offset >= decoded.buffer.length || power > 49)
                            fail();
                        part = decoded.buffer[offset++]!;
                        n += (part & 127) * 2 ** power;
                        power += 7;
                    } while (part & 128); return n; };
                    const sourceSize = variable(), targetSize = variable();
                    if (sourceSize > OBJECT_LIMIT)
                        fail();
                    account(Math.max(targetSize, size));
                }
                else
                    account(size);
            }
            if (at !== end)
                fail();
            return true;
        }
        if (/^objects\/pack\/pack-[a-f0-9]+\.idx$/.test(path)) {
            const v2 = bytes.subarray(0, 4).equals(Buffer.from([255, 116, 79, 99]));
            let at = v2 ? 8 : 0;
            if (v2 && (bytes.length < 8 || bytes.readUInt32BE(4) !== 2))
                fail();
            if (bytes.length < at + 1024 + 2 * hashLength)
                fail();
            let previous = 0;
            for (let i = 0; i < 256; i++) {
                const n = bytes.readUInt32BE(at + i * 4);
                if (n < previous || n > COUNT_LIMIT)
                    fail();
                previous = n;
            }
            at += 1024;
            const minimal = at + previous * (v2 ? hashLength + 8 : hashLength + 4) + 2 * hashLength;
            if (bytes.length < minimal || bytes.length - minimal > previous * 8 || (v2 ? (bytes.length - minimal) % 8 !== 0 : bytes.length !== minimal))
                fail();
            if (!createHash(hashLength === 32 ? "sha256" : "sha1").update(bytes.subarray(0, -hashLength)).digest().equals(bytes.subarray(-hashLength)))
                fail();
            return true;
        }
        // Commit graphs, bitmaps, reverse indexes and config are preserved only as inert archive bytes.
        return false;
    };
}
