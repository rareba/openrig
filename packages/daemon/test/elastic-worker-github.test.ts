import { describe, it, expect, afterEach } from 'vitest';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { generateKeyPairSync } from 'node:crypto';
import { sealGithubMaterial, openGithubMaterial, sealWorkerMaterial } from '../src/domain/elastic-enrollment.js';
import { GithubCredentialStore, repositoryPlanSchema, credentialReadiness, githubChildEnvironment } from '../src/adapters/elastic-worker-github.js';
const roots: string[] = [];
afterEach(() => roots.splice(0).forEach(p => rmSync(p, { recursive: true, force: true })));
const binding = { version: 1 as const, resourceId: '/subscriptions/fixture/resourceGroups/fixture/providers/Microsoft.Compute/virtualMachineScaleSets/fixture', vmId: '11111111-1111-4111-8111-111111111111', nonce: Buffer.alloc(32).toString('base64'), manifestDigest: 'a'.repeat(64), operationId: 'fixture', fleetEpoch: 'fixture' };
const repo = { id: 123, name: 'fixture/repo', permissions: { contents: 'read' as const, issues: 'none' as const, pullRequests: 'none' as const } };
describe('optional scoped Github material', () => {
    it('separates encrypted AAD and rotates atomic single-file revisions without restart', () => {
        const key = generateKeyPairSync('rsa', { modulusLength: 3072 });
        const bytes = Buffer.from('fixture');
        expect(openGithubMaterial(sealGithubMaterial(bytes, binding, key.publicKey), binding, key.privateKey)).toEqual(bytes);
        expect(() => openGithubMaterial(sealWorkerMaterial(bytes, binding, key.publicKey), binding, key.privateKey)).toThrow();
        const root = realpathSync(mkdtempSync(join(tmpdir(), 'github-')));
        roots.push(root);
        const s = new GithubCredentialStore(root, process.getuid!(), process.getgid!());
        const material = { version: 1 as const, token: 'fixture-token-one', expectedPreviousRevision: null, revision: 'one', expiresAt: Date.now() + 60000, repositories: [repo] };
        expect(s.install(material).revision).toBe('one');
        expect(s.install(material).revision).toBe('one');
        expect(() => s.install({ ...material, token: 'conflict' })).toThrow();
        expect(s.install({ ...material, token: 'fixture-token-two', revision: 'two', expectedPreviousRevision: 'one' }).revision).toBe('two');
        const env = githubChildEnvironment(s.read(), { GH_TOKEN: 'evil', GITHUB_TOKEN: 'evil', GH_HOST: 'evil', HOME: root });
        expect(env.GH_TOKEN).toBe('fixture-token-two');
        expect(env.GITHUB_TOKEN).toBeUndefined();
        expect(env.GH_HOST).toBe('github.com');
    });
    it('makes public/private and expiry capabilities explicit', () => {
        expect(repositoryPlanSchema.parse({ mode: 'public-readonly' })).toEqual({ mode: 'public-readonly' });
        expect(credentialReadiness({ mode: 'public-readonly' }, null, Date.now())).toEqual({ state: 'public-readonly', privateRead: false, write: false });
        const plan = { mode: 'token' as const, credentialSourceRef: '/private/fixture', credentialRevision: 'one', expiresAt: 1, repositories: [repo] };
        expect(credentialReadiness(plan, null, Date.now()).state).toBe('unavailable');
    });
});
