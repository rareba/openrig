import { createHash } from 'node:crypto';
import { closeSync, constants, fstatSync, openSync, readSync, createReadStream } from 'node:fs';
import type { IncomingMessage, ServerResponse } from 'node:http';
/** Validate the inert uncompressed ustar artifact before serving a pinned descriptor. */
export function openRuntimeArtifact(input: {
    path: string;
    archiveSha256: string;
    archiveBytes: number;
}) {
    if (!Number.isSafeInteger(input.archiveBytes) || input.archiveBytes < 1024 || input.archiveBytes > 3 * 1024 ** 3 || input.archiveBytes % 512 !== 0)
        throw Error('elastic_artifact_rejected');
    const fd = openSync(input.path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
        const stat = fstatSync(fd);
        if ((stat.uid !== 0 && stat.uid !== process.getuid?.()) || !stat.isFile() || stat.nlink !== 1 || stat.mode & 0o022 || stat.size !== input.archiveBytes || !/^[a-f0-9]{64}$/.test(input.archiveSha256))
            throw Error();
        const hash = createHash('sha256');
        const buffer = Buffer.alloc(1024 * 1024);
        for (let offset = 0; offset < stat.size;) {
            const n = readSync(fd, buffer, 0, Math.min(buffer.length, stat.size - offset), offset);
            if (!n)
                throw Error();
            hash.update(buffer.subarray(0, n));
            offset += n;
        }
        if (hash.digest('hex') !== input.archiveSha256)
            throw Error();
        const header = Buffer.alloc(512);
        const names = new Set<string>();
        let ended = false;
        for (let offset = 0; offset < stat.size;) {
            if (readSync(fd, header, 0, 512, offset) !== 512)
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
            let suffix = '';
            for (let position = offset; position < offset + size;) {
                const n = readSync(fd, buffer, 0, Math.min(buffer.length, offset + size - position), position);
                if (!n)
                    throw Error();
                const text = suffix + buffer.subarray(0, n).toString('latin1');
                if (/-----BEGIN ((?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY)-----[\r\n]+[A-Za-z0-9+/=\r\n]{64,}-----END \1-----/.test(text))
                    throw Error();
                suffix = text.slice(-32768);
                position += n;
            }
            names.add(name);
            offset += Math.ceil(size / 512) * 512;
        }
        if (!ended || !names.has('provenance.json') || fstatSync(fd).mtimeMs !== stat.mtimeMs || fstatSync(fd).ctimeMs !== stat.ctimeMs)
            throw Error();
        let active = 0;
        const streams = new Set<ReturnType<typeof createReadStream>>();
        let closed = false;
        const unchanged = () => { const next = fstatSync(fd); return next.ino === stat.ino && next.dev === stat.dev && next.size === stat.size && next.mtimeMs === stat.mtimeMs && next.ctimeMs === stat.ctimeMs; };
        const route = `/api/elastic-artifacts/${input.archiveSha256}`;
        return {
            path: route,
            handle(req: IncomingMessage, res: ServerResponse): void {
                if (closed || req.url !== route || !['GET', 'HEAD'].includes(req.method ?? '') || req.headers.range || !unchanged()) {
                    res.writeHead(404, { connection: 'close' });
                    res.end();
                    return;
                }
                if (active >= 2) {
                    res.writeHead(503, { connection: 'close' });
                    res.end();
                    return;
                }
                res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': stat.size, etag: `"${input.archiveSha256}"`, connection: 'close' });
                if (req.method === 'HEAD') {
                    res.end();
                    return;
                }
                active++;
                const stream = createReadStream(input.path, { fd, autoClose: false, start: 0, end: stat.size - 1 });
                streams.add(stream);
                const timer = setTimeout(() => { stream.destroy(); res.destroy(); }, 30000);
                let finished = false;
                const finish = () => { if (finished)
                    return; finished = true; clearTimeout(timer); active--; streams.delete(stream); stream.destroy(); };
                res.once('close', finish);
                stream.once('error', () => res.destroy());
                stream.pipe(res);
            },
            close() { if (!closed) {
                closed = true;
                for (const stream of streams) stream.destroy();
                closeSync(fd);
            } },
        };
    }
    catch {
        closeSync(fd);
        throw Error('elastic_artifact_rejected');
    }
}
export type RuntimeArtifact = ReturnType<typeof openRuntimeArtifact>;
