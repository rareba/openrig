import { afterEach, expect, it, vi } from 'vitest';
import { mkdtempSync, realpathSync, chmodSync, writeFileSync, readFileSync, rmSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash, generateKeyPairSync } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { createServer, request } from 'node:https';
import { once } from 'node:events';
import { Writable } from 'node:stream';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { MAX_RUNTIME_ARTIFACT_BYTES, openRuntimeArtifact } from '../src/adapters/elastic-runtime-artifact.js';
import { privateTransferHandler } from '../src/adapters/elastic-checkpoint-receiver.js';
import { CheckpointCustody } from '../src/adapters/elastic-checkpoint-custody.js';
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0))
    await close(); vi.restoreAllMocks(); });
function fixture(secret: boolean | string = false) {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'elastic-artifact-')));
    chmodSync(root, 0o700);
    cleanups.push(() => rmSync(root, { recursive: true, force: true }));
    mkdirSync(join(root, 'openrig'));
    writeFileSync(join(root, 'openrig', 'index.js'), typeof secret === 'string' ? secret : secret ? generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ type: 'pkcs8', format: 'pem' }) : 'const header = "-----BEGIN PRIVATE KEY-----";');
    writeFileSync(join(root, 'provenance.json'), '{}');
    const archive = join(root, 'artifact.tar');
    execFileSync('/usr/bin/tar', ['--format=ustar', '-cf', archive, '-C', root, 'openrig', 'provenance.json']);
    chmodSync(archive, 0o600);
    const bytes = readFileSync(archive);
    return { root, bytes, descriptor: { path: archive, archiveSha256: createHash('sha256').update(bytes).digest('hex'), archiveBytes: bytes.length } };
}
it('serves only exact immutable artifact bytes on actual TLS alongside capability-protected checkpoint routing', async () => {
    const f = fixture();
    const artifact = openRuntimeArtifact(f.descriptor);
    cleanups.push(() => artifact.close());
    const custodyDir = join(f.root, 'custody');
    mkdirSync(custodyDir, { mode: 0o700 });
    const custody = new CheckpointCustody({ directory: custodyDir, durableStoreId: 'fixture', maxBytes: 1024 * 1024, maxReservedBytes: 1024 * 1024, maxStoredBytes: 16 * 1024 * 1024, retentionPolicyId: 'fixture', maxCheckpoints: 2 });
    execFileSync('/usr/bin/openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', join(f.root, 'tls.key'), '-out', join(f.root, 'tls.cert'), '-days', '1', '-subj', '/CN=fixture', '-addext', 'subjectAltName=IP:127.0.0.1'], { stdio: 'pipe' });
    const cert = readFileSync(join(f.root, 'tls.cert'));
    const server = createServer({ key: readFileSync(join(f.root, 'tls.key')), cert, minVersion: 'TLSv1.3', maxVersion: 'TLSv1.3' }, privateTransferHandler(custody, artifact));
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    cleanups.unshift(() => new Promise<void>(resolve => { server.close(() => resolve()); server.closeAllConnections(); }));
    const address = server.address();
    if (!address || typeof address === 'string')
        throw Error('fixture');
    const get = (path: string, method = 'GET') => new Promise<{
        status: number;
        body: Buffer;
    }>(resolve => { const req = request({ host: '127.0.0.1', port: address.port, path, method, ca: cert, minVersion: 'TLSv1.3' }, res => { const chunks: Buffer[] = []; res.on('data', chunk => chunks.push(chunk)); res.on('end', () => resolve({ status: res.statusCode!, body: Buffer.concat(chunks) })); }); req.end(); });
    expect(await get(artifact.path)).toEqual({ status: 200, body: f.bytes });
    expect((await get(artifact.path, 'HEAD')).body.length).toBe(0);
    for (const path of [artifact.path + '?x=1', '/api/elastic-artifacts/../custody', '/api/elastic-artifacts/' + 'a'.repeat(64), '/api/elastic-fleet'])
        expect((await get(path)).status).not.toBe(200);
    expect((await get('/api/elastic-checkpoints/' + 'a'.repeat(64), 'PUT')).status).toBe(400);
    writeFileSync(f.descriptor.path, Buffer.alloc(f.bytes.length));
    expect(await get(artifact.path)).toEqual({ status: 200, body: f.bytes });
});
it('rejects changed digests and private key material before publication', () => {
    const f = fixture();
    expect(() => openRuntimeArtifact({ ...f.descriptor, archiveSha256: 'a'.repeat(64) })).toThrow('elastic_artifact_rejected');
    for (const archiveBytes of [0, -1, Number.NaN, 3 * 1024 ** 3 + 512, f.bytes.length + 1])
        expect(() => openRuntimeArtifact({ ...f.descriptor, archiveBytes })).toThrow('elastic_artifact_rejected');
    const secret = fixture(true);
    expect(() => openRuntimeArtifact(secret.descriptor)).toThrow('elastic_artifact_rejected');
});

it('never streams bytes written to the original inode after GET has begun', async () => {
    const f = fixture('original artifact bytes '.repeat(100000));
    const artifact = openRuntimeArtifact(f.descriptor);
    cleanups.push(() => artifact.close());
    const chunks: Buffer[] = [];
    let status = 0;
    const sink = Object.assign(new Writable({ write(chunk, _encoding, done) { chunks.push(Buffer.from(chunk)); done(); } }), {
        writeHead(code: number) { status = code; return this; },
    });
    const completed = once(sink, 'finish');
    artifact.handle({ url: artifact.path, method: 'GET', headers: {} } as IncomingMessage, sink as unknown as ServerResponse);
    writeFileSync(f.descriptor.path, Buffer.alloc(f.bytes.length, 88));
    await completed;
    expect(status).toBe(200);
    expect(Buffer.concat(chunks).equals(f.bytes)).toBe(true);
});
it('rejects complete traditional encrypted PKCS1 PEM including its standard encryption headers', () => {
    const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey;
    const pem = key.export({ type: 'pkcs1', format: 'pem', cipher: 'aes-256-cbc', passphrase: 'fixture-only' }).toString();
    expect(pem).toContain('Proc-Type: 4,ENCRYPTED');
    expect(pem).toContain('DEK-Info:');
    for (const content of [pem, 'x'.repeat(1024 ** 2 - 16) + '\n' + pem]) {
        const f = fixture(content);
        expect(() => { const artifact = openRuntimeArtifact(f.descriptor); artifact.close(); }).toThrow('elastic_artifact_rejected');
    }
});

it('refuses pre-capture changes, excessive size and insufficient process memory before publication', () => {
    const f = fixture();
    expect(() => openRuntimeArtifact({ ...f.descriptor, archiveBytes: MAX_RUNTIME_ARTIFACT_BYTES + 512 })).toThrow('elastic_artifact_rejected');
    const available = vi.spyOn(process, 'availableMemory').mockReturnValue(0);
    expect(() => openRuntimeArtifact(f.descriptor)).toThrow('elastic_artifact_memory_unavailable');
    available.mockRestore();
    writeFileSync(f.descriptor.path, Buffer.alloc(f.bytes.length, 88));
    expect(() => openRuntimeArtifact(f.descriptor)).toThrow('elastic_artifact_rejected');
});
it('bounds concurrent snapshot streams and closes them without retaining an open source descriptor', async () => {
    const f = fixture('large fixture '.repeat(100000));
    const artifact = openRuntimeArtifact(f.descriptor);
    cleanups.push(() => artifact.close());
    const sink = () => {
        let began!: () => void;
        const started = new Promise<void>(resolve => { began = resolve; });
        return Object.assign(new Writable({ write() { began(); /* Deliberate consumer backpressure. */ } }), {
            started, status: 0, writeHead(code: number) { this.status = code; return this; },
        });
    };
    const first = sink(), second = sink(), overflow = sink();
    const get = (response: ReturnType<typeof sink>) => artifact.handle({ url: artifact.path, method: 'GET', headers: {} } as IncomingMessage, response as unknown as ServerResponse);
    get(first); get(second); get(overflow);
    expect([first.status, second.status, overflow.status]).toEqual([200, 200, 503]);
    await Promise.all([first.started, second.started]);
    const closed = Promise.all([once(first, 'close'), once(second, 'close')]);
    artifact.close(); artifact.close();
    await closed;
    const after = sink(); get(after); expect(after.status).toBe(404);
});
it('rejects complete EC, DSA and encrypted PKCS8 keys while allowing source header constants', () => {
    const ec = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }).privateKey;
    const dsa = generateKeyPairSync('dsa', { modulusLength: 1024, divisorLength: 160 }).privateKey;
    const keys = [
        ec.export({ type: 'sec1', format: 'pem' }).toString(),
        ec.export({ type: 'pkcs8', format: 'pem', cipher: 'aes-256-cbc', passphrase: 'fixture' }).toString(),
        dsa.export({ type: 'pkcs8', format: 'pem' }).toString(),
    ];
    // OpenSSL's traditional DSA label carries the same complete PEM framing.
    const dsaTraditional = execFileSync('/usr/bin/openssl', ['dsa'], { input: keys[2], encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] });
    expect(dsaTraditional).toContain('BEGIN DSA PRIVATE KEY');
    for (const key of [...keys, dsaTraditional]) {
        const f = fixture(key);
        expect(() => { const artifact = openRuntimeArtifact(f.descriptor); artifact.close(); }).toThrow('elastic_artifact_rejected');
    }
    const literals = fixture('const headers = ["-----BEGIN RSA PRIVATE KEY-----", "-----BEGIN DSA PRIVATE KEY-----", "-----BEGIN EC PRIVATE KEY-----"];');
    openRuntimeArtifact(literals.descriptor).close();
});

it.each(['pkcs8', 'encrypted-pkcs1'] as const)('rejects complete %s keys embedded in source independently of surrounding text', format => {
    const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey;
    const pem = (format === 'pkcs8'
        ? key.export({ type: 'pkcs8', format: 'pem' })
        : key.export({ type: 'pkcs1', format: 'pem', cipher: 'aes-256-cbc', passphrase: 'fixture' })).toString().trimEnd();
    const template = 'export const key = `' + pem + '`;';
    const splitFooter = ' '.repeat(1024 ** 2 - template.indexOf('-----END') - 10) + template;
    for (const source of [template, 'source prefix ' + pem + ' source suffix', splitFooter]) {
        const f = fixture(source);
        expect(() => { const artifact = openRuntimeArtifact(f.descriptor); artifact.close(); }).toThrow('elastic_artifact_rejected');
    }
});
