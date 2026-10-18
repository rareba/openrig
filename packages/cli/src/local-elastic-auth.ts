import { constants, closeSync, fstatSync, lstatSync, openSync, readFileSync, realpathSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';
function trustedRead(path: string): string {
    for (let at = dirname(path);; at = dirname(at)) {
        const s = lstatSync(at);
        if (!s.isDirectory() || s.uid !== 0 || (s.mode & 0o022) || realpathSync(at) !== at)
            throw Error();
        if (at === '/')
            break;
    }
    const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
        const s = fstatSync(fd);
        if (!s.isFile() || s.nlink !== 1 || s.uid !== 0 || (s.mode & 0o022) || s.size > 65536 || (path.endsWith('local-api.json') && (s.mode & 0o007)))
            throw Error();
        return readFileSync(fd, 'utf8');
    }
    finally {
        closeSync(fd);
    }
}
/** Only root-controlled local installation records are authority, never environment token values. */
export function resolveLocalElasticApiCredential(request: {
    enabled: boolean;
    baseUrl: string;
    remoteTarget: boolean;
    explicitUrl: boolean;
}, read = trustedRead): string | undefined {
    if (!request.enabled || request.remoteTarget || request.explicitUrl)
        return undefined;
    try {
        const raw = read('/etc/openrig-elastic/manifest.json');
        const digest = createHash('sha256').update(raw).digest('hex');
        if (read('/etc/openrig-elastic/manifest.sha256').trim() !== digest)
            throw Error();
        const manifest = JSON.parse(raw);
        if (manifest.linux.guard !== '/etc/openrig-elastic/guard.json' || manifest.linux.identity !== '/etc/openrig-elastic/instance.json')
            throw Error();
        const guard = JSON.parse(read(manifest.linux.guard));
        const local = JSON.parse(read('/etc/openrig-elastic/local-api.json'));
        const url = new URL(local.baseUrl);
        const ip = url.hostname.split('.').map(Number);
        if (url.protocol !== 'http:' || url.username || url.password || url.pathname !== '/' || url.search || url.hash || url.port !== String(manifest.port) || ip.length !== 4 || ip.some((n, i) => !Number.isInteger(n) || n < 0 || n > 255 || String(n) !== url.hostname.split('.')[i]) || !(ip[0] === 10 || ip[0] === 192 && ip[1] === 168 || ip[0] === 172 && ip[1]! >= 16 && ip[1]! <= 31))
            throw Error();
        const expectedUrl = `http://${url.hostname}:${manifest.port}`;
        if (request.baseUrl !== expectedUrl)
            return undefined;
        if (guard.phase !== 'ready' || local.version !== 1 || local.baseUrl !== expectedUrl || local.vmId !== guard.vmId || local.workerId !== guard.workerId
            || local.manifestDigest !== digest || guard.manifestDigest !== digest || !/^[A-Za-z0-9_-]{32,256}$/.test(local.bearer))
            throw Error();
        return local.bearer;
    }
    catch {
        throw Error('elastic_local_auth_unavailable');
    }
}
