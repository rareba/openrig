import { describe, it, expect } from 'vitest';
import { resolveLocalElasticApiCredential } from '../src/local-elastic-auth.js';
import { createHash } from 'node:crypto';
const manifest = JSON.stringify({ port: 7433, linux: { guard: '/etc/openrig-elastic/guard.json', identity: '/etc/openrig-elastic/instance.json' } });
const digest = createHash('sha256').update(manifest).digest('hex');
const workerId = 'azure:fixture:vm';
const files: Record<string, string> = { '/etc/openrig-elastic/manifest.json': manifest, '/etc/openrig-elastic/manifest.sha256': digest,
    '/etc/openrig-elastic/guard.json': JSON.stringify({ version: 1, phase: 'ready', workerId, vmId: 'vm', manifestDigest: digest }),
    '/etc/openrig-elastic/instance.json': JSON.stringify({ version: 1, vmId: 'vm', manifestDigest: digest, privateIPv4: '10.0.0.2' }),
    '/etc/openrig-elastic/local-api.json': JSON.stringify({ version: 1, workerId, vmId: 'vm', manifestDigest: digest, baseUrl: 'http://10.0.0.2:7433', bearer: 'a'.repeat(64) }) };
const read = (p: string) => { if (!(p in files))
    throw Error(); return files[p]!; };
describe('local elastic client bearer', () => {
    it('uses exact root-bound local endpoint only and fails closed if credential is missing', () => {
        const request = { enabled: true, baseUrl: 'http://10.0.0.2:7433', remoteTarget: false, explicitUrl: false };
        expect(resolveLocalElasticApiCredential(request, read)).toBe('a'.repeat(64));
        for (const changed of [{ baseUrl: 'http://127.0.0.1:7433' }, { baseUrl: 'http://10.0.0.2:7433/' }, { remoteTarget: true }, { explicitUrl: true }, { enabled: false }])
            expect(resolveLocalElasticApiCredential({ ...request, ...changed }, read)).toBeUndefined();
        expect(() => resolveLocalElasticApiCredential(request, p => { if (p.endsWith('local-api.json'))
            throw Error(); return read(p); })).toThrow('elastic_local_auth_unavailable');
    });
});
