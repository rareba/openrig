import { afterEach, describe, expect, it } from 'vitest';
import { mkdtempSync, rmSync, chmodSync, symlinkSync, readFileSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createPublicKey } from 'node:crypto';
import { ElasticWorkerAttemptStore } from '../src/adapters/elastic-worker-attempts.js';
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true }); });
function store() { const root = realpathSync(mkdtempSync(join(tmpdir(), 'enrollment-'))); chmodSync(root, 0o700); roots.push(root); return new ElasticWorkerAttemptStore(root); }
export const identity = { resourceId: '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture', childResourceId: '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture/virtualMachines/0', instanceId: '0', vmId: '11111111-1111-4111-8111-111111111111', operationId: 'fixture-operation', fleetEpoch: 'fixture-epoch', manifestDigest: 'a'.repeat(64), artifactDigest: 'b'.repeat(64), specDigest: 'c'.repeat(64), checkoutDigest: 'd'.repeat(64), configurationDigest: 'e'.repeat(64) };
describe('durable private enrollment attempts', () => {
    it('recovers the original nonce and RSA key and compare-revision state after restart', () => {
        const first = store();
        const a = first.createOrRead(identity);
        const second = new ElasticWorkerAttemptStore(first.root);
        const b = second.createOrRead(identity);
        expect(b).toEqual(a);
        expect(createPublicKey(first.privateKey(a.id)).asymmetricKeyDetails?.modulusLength).toBeGreaterThanOrEqual(3072);
        expect(second.privateKey(a.id)).toEqual(first.privateKey(a.id));
        const advanced = second.advance(a.id, 0, { phase: 'staging', command: { name: 'fixture-command', operation: 'stage', requestDigest: 'f'.repeat(64), state: 'intent' } });
        expect(first.read(a.id)).toEqual(advanced);
        expect(() => first.advance(a.id, 0, { phase: 'installed' })).toThrow();
        expect(() => first.advance(a.id, 1, { phase: 'prepared', command: { name: 'other', operation: 'prepare', requestDigest: '0'.repeat(64), state: 'intent' } })).toThrow();
        expect(() => first.advance(a.id, 1, { command: null })).toThrow();
        expect(first.listPending()).toHaveLength(1);
        expect(JSON.stringify(a)).not.toContain('PRIVATE KEY');
        expect(readFileSync(first.tokenPath(a.id), 'utf8')).toBe('');
    });
    it('refuses changed incarnation configuration and symlink or permissive directories', () => {
        const s = store();
        s.createOrRead(identity);
        expect(() => s.createOrRead({ ...identity, artifactDigest: '0'.repeat(64) })).toThrow();
        const link = `${s.root}-link`;
        roots.push(link);
        symlinkSync(s.root, link);
        expect(() => new ElasticWorkerAttemptStore(link)).toThrow();
        chmodSync(s.root, 0o755);
        expect(() => s.read('f'.repeat(64))).toThrow();
    });
});
