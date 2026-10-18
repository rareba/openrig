import { createHash } from 'node:crypto';
import { closeSync, constants, fstatSync, openSync, readSync } from 'node:fs';
import { Blob } from 'node:buffer';
import { Readable } from 'node:stream';
import type { IncomingMessage, ServerResponse } from 'node:http';
export const MAX_RUNTIME_ARTIFACT_BYTES = 1024 ** 3;
const chunkBytes = 1024 ** 2;
const memoryOverhead = 256 * 1024 ** 2;
type ArtifactInput = { path: string; archiveSha256: string; archiveBytes: number };

/** Capture, validate and copy into an immutable Blob before exposing any response. */
function validatedSnapshot(input: ArtifactInput): Blob {
    if (!Number.isSafeInteger(input.archiveBytes) || input.archiveBytes < 1024 || input.archiveBytes > MAX_RUNTIME_ARTIFACT_BYTES || input.archiveBytes % 512 !== 0)
        throw Error('elastic_artifact_rejected');
    // This is a conservative process/cgroup-aware preflight, not a memory reservation.
    // Capture buffers and the immutable Blob can coexist until garbage collection.
    if (process.availableMemory() < 2 * input.archiveBytes + memoryOverhead)
        throw Error('elastic_artifact_memory_unavailable');
    const fd = openSync(input.path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
        const stat = fstatSync(fd);
        if ((stat.uid !== 0 && stat.uid !== process.getuid?.()) || !stat.isFile() || stat.nlink !== 1 || stat.mode & 0o022 || stat.size !== input.archiveBytes || !/^[a-f0-9]{64}$/.test(input.archiveSha256))
            throw Error();
        const chunks: Buffer[] = [];
        const hash = createHash('sha256');
        for (let offset = 0; offset < stat.size;) {
            const chunk = Buffer.allocUnsafe(Math.min(chunkBytes, stat.size - offset));
            let copied = 0;
            while (copied < chunk.length) {
                const count = readSync(fd, chunk, copied, chunk.length - copied, offset + copied);
                if (!count) throw Error();
                copied += count;
            }
            chunks.push(chunk);
            hash.update(chunk);
            offset += chunk.length;
        }
        if (hash.digest('hex') !== input.archiveSha256) throw Error();
        // All subsequent validation reads the captured bytes, never the live inode.
        const read = (target: Buffer, count: number, offset: number): number => {
            if (offset + count > stat.size) return 0;
            let copied = 0;
            while (copied < count) {
                const at = offset + copied;
                const chunk = chunks[Math.floor(at / chunkBytes)]!;
                const start = at % chunkBytes;
                const length = Math.min(count - copied, chunk.length - start);
                chunk.copy(target, copied, start, start + length);
                copied += length;
            }
            return copied;
        };
        const buffer = Buffer.alloc(chunkBytes);
        const header = Buffer.alloc(512);
        const names = new Set<string>();
        let ended = false;
        for (let offset = 0; offset < stat.size;) {
            if (read(header, 512, offset) !== 512)
                throw Error();
            offset += 512;
            if (header.every(b => b === 0)) {
                ended = true;
                continue;
            }
            if (ended)
                throw Error();
            const string = (start: number, length: number) => header.subarray(start, start + length).toString('utf8').replace(/\0.*$/s, '');
            const checksumText = string(148, 8).trim();
            if (!/^[0-7]+$/.test(checksumText))
                throw Error();
            const checksum = header.reduce((sum, value, index) => sum + (index >= 148 && index < 156 ? 32 : value), 0);
            if (checksum !== parseInt(checksumText, 8) || string(257, 6) !== 'ustar')
                throw Error();
            const name = [string(345, 155), string(0, 100)].filter(Boolean).join('/');
            const type = string(156, 1);
            const sizeText = string(124, 12).trim();
            if (!/^[0-7]+$/.test(sizeText))
                throw Error();
            const size = parseInt(sizeText, 8);
            const parts = name.replace(/\/$/, '').split('/');
            if (!['openrig', 'pi', 'node', 'os-packages', 'provenance.json'].includes(parts[0]!) || names.has(name) || parts.some(p => !p || p === '.' || p === '..' || /[\\\x00-\x1f]/.test(p))
                || !['', '0', '5'].includes(type) || (type === '5' && size !== 0) || size > 512 * 1024 ** 2 || offset + size > stat.size
                || parts.some(p => ['.git', '.env', '.npmrc', '.codex', '.claude', '.openrig', 'auth.json', 'hosts.yaml', 'instance.json'].includes(p))
                || /\.(?:sqlite|sqlite3|db|key|p12)$/.test(name))
                throw Error();
            if (names.size >= 250000)
                throw Error();
            const inspect = privateKeyScanner();
            for (let position = offset; position < offset + size;) {
                const n = read(buffer, Math.min(buffer.length, offset + size - position), position);
                if (!n)
                    throw Error();
                inspect(buffer.subarray(0, n).toString('latin1'));
                position += n;
            }
            inspect('\n');
            names.add(name);
            offset += Math.ceil(size / 512) * 512;
        }
        if (!ended || !names.has('provenance.json')) throw Error();
        return new Blob(chunks);
    } catch {
        throw Error('elastic_artifact_rejected');
    } finally {
        closeSync(fd);
    }
}

/** Complete PEM blocks, including traditional encryption headers; literals alone are inert. */
function privateKeyScanner(): (chunk: string) => void {
    let pending = '', label = '', body = 0;
    const line = (value: string) => {
        value = value.trim();
        if (label) {
            if (value === `-----END ${label}-----` && body >= 64) throw Error();
            if (/^[A-Za-z0-9+/=]+$/.test(value)) { body += value.length; return; }
            if (value === '' || (!body && /^(?:Proc-Type|DEK-Info):[^\r\n]+$/.test(value))) return;
            label = ''; body = 0;
        }
        label = /-----BEGIN ((?:[A-Z0-9]+ )*PRIVATE KEY)-----$/.exec(value)?.[1] ?? '';
    };
    return chunk => {
        const text = pending + chunk;
        let start = 0, end: number;
        while ((end = text.indexOf('\n', start)) !== -1) {
            line(text.slice(start, end));
            start = end + 1;
        }
        pending = text.slice(start);
        if (pending.length > 16384) {
            // Refuse an oversized candidate rather than discard part of a possible key.
            if (label) throw Error();
            pending = pending.slice(-512);
        }
    };
}

/** At most two downloads of a validated immutable snapshot; the source fd is already closed. */
export function openRuntimeArtifact(input: ArtifactInput) {
    let snapshot: Blob | null = validatedSnapshot(input);
    const streams = new Set<Readable>();
    const route = `/api/elastic-artifacts/${input.archiveSha256}`;
    return {
        path: route,
        handle(req: IncomingMessage, res: ServerResponse): void {
            if (!snapshot || req.url !== route || !['GET', 'HEAD'].includes(req.method ?? '') || req.headers.range) {
                res.writeHead(404, { connection: 'close' }); res.end(); return;
            }
            if (streams.size >= 2) {
                res.writeHead(503, { connection: 'close' }); res.end(); return;
            }
            res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': snapshot.size, etag: `"${input.archiveSha256}"`, connection: 'close' });
            if (req.method === 'HEAD') { res.end(); return; }
            const stream = Readable.fromWeb(snapshot.stream());
            streams.add(stream);
            const timer = setTimeout(() => { stream.destroy(); res.destroy(); }, 30000);
            const finish = () => { clearTimeout(timer); streams.delete(stream); stream.destroy(); };
            res.once('close', finish);
            stream.once('close', () => { clearTimeout(timer); streams.delete(stream); if (!res.writableFinished) res.destroy(); });
            stream.once('error', () => res.destroy());
            stream.pipe(res);
        },
        close() {
            snapshot = null;
            for (const stream of streams) stream.destroy();
        },
    };
}
export type RuntimeArtifact = ReturnType<typeof openRuntimeArtifact>;
