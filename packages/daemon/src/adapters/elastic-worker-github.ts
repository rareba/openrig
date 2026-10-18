import { z } from 'zod';
import { closeSync, constants, existsSync, fchmodSync, fchownSync, fsyncSync, fstatSync, lstatSync, openSync, readFileSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { CheckpointPrivateStore } from './elastic-checkpoint-private-store.js';
import { pathToFileURL } from 'node:url';
import { enrollmentDigest } from './elastic-worker-attempts.js';
const revision = z.string().regex(/^[A-Za-z0-9_-]{1,128}$/);
export const githubRepositorySchema = z.object({ id: z.number().int().positive(), name: z.string().regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/), permissions: z.object({ contents: z.enum(['read', 'write']), issues: z.enum(['none', 'read', 'write']), pullRequests: z.enum(['none', 'read', 'write']) }).strict() }).strict();
const repositories = z.array(githubRepositorySchema).min(1).max(32).refine(items => new Set(items.map(r => r.id)).size === items.length && new Set(items.map(r => r.name.toLowerCase())).size === items.length);
export const repositoryPlanSchema = z.discriminatedUnion('mode', [z.object({ mode: z.literal('public-readonly') }).strict(), z.object({ mode: z.literal('token'), credentialSourceRef: z.string().max(1024).refine(p => p.startsWith('/') && resolve(p) === p), credentialRevision: revision, expiresAt: z.number().int().positive(), repositories }).strict()]);
export type RepositoryAccessPlan = z.infer<typeof repositoryPlanSchema>;
export const githubMaterialSchema = z.object({ version: z.literal(1), token: z.string().min(8).max(1024).regex(/^[A-Za-z0-9_.-]+$/), expectedPreviousRevision: revision.nullable(), revision, expiresAt: z.number().int().positive(), repositories }).strict();
export type GithubMaterial = z.infer<typeof githubMaterialSchema>;
const storedSchema = z.object({ material: githubMaterialSchema, materialDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict();
function reject(): never { throw Error('elastic_github_credential_unavailable'); }
/** Fixed file on the worker. Alternate directory/owners are only used by isolated filesystem fixtures. */
export class GithubCredentialStore {
    constructor(private readonly directory = '/etc/openrig-elastic', private readonly owner = 0, private readonly group = 0) { }
    private verifyDirectory(): void {
        const s = lstatSync(this.directory);
        if (realpathSync(this.directory) !== this.directory || !s.isDirectory() || s.uid !== this.owner || (s.mode & 0o022))
            reject();
        for (let at = dirname(this.directory);; at = dirname(at)) {
            const a = lstatSync(at);
            const sticky = a.uid === 0 && (a.mode & 0o1000);
            if (!a.isDirectory() || realpathSync(at) !== at || (a.uid !== 0 && a.uid !== this.owner) || ((a.mode & 0o022) && !sticky))
                reject();
            if (at === '/')
                break;
        }
    }
    read(): GithubMaterial | null {
        this.verifyDirectory();
        const path = join(this.directory, 'github.json');
        if (!existsSync(path))
            return null;
        const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
        try {
            const s = fstatSync(fd);
            if (!s.isFile() || s.nlink !== 1 || s.uid !== this.owner || (s.mode & 0o027) || s.size > 16384)
                reject();
            const stored = storedSchema.parse(JSON.parse(readFileSync(fd, 'utf8')));
            if (stored.materialDigest !== enrollmentDigest(stored.material))
                reject();
            return stored.material;
        }
        finally {
            closeSync(fd);
        }
    }
    install(input: GithubMaterial) {
        const material = githubMaterialSchema.parse(input);
        this.verifyDirectory();
        const current = this.read();
        const materialDigest = enrollmentDigest(material);
        if (current?.revision === material.revision) {
            if (enrollmentDigest(current) !== materialDigest)
                reject();
            return { revision: material.revision, expiresAt: material.expiresAt, materialDigest };
        }
        if ((current?.revision ?? null) !== material.expectedPreviousRevision || material.expiresAt <= Date.now())
            reject();
        const path = join(this.directory, 'github.json');
        const temp = join(this.directory, `github-${randomBytes(16).toString('hex')}.tmp`);
        const fd = openSync(temp, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
        try {
            writeFileSync(fd, JSON.stringify({ material, materialDigest }));
            fchownSync(fd, this.owner, this.group);
            fchmodSync(fd, 0o640);
            fsyncSync(fd);
        }
        finally {
            closeSync(fd);
        }
        renameSync(temp, path);
        const directory = openSync(this.directory, constants.O_RDONLY | constants.O_NOFOLLOW);
        try {
            fsyncSync(directory);
        }
        finally {
            closeSync(directory);
        }
        return { revision: material.revision, expiresAt: material.expiresAt, materialDigest };
    }
}
export function readGithubCredentialSource(source: string): GithubMaterial {
    if (!source.startsWith('/') || resolve(source) !== source)
        reject();
    const privateFiles = new CheckpointPrivateStore(dirname(source));
    return githubMaterialSchema.parse(JSON.parse(privateFiles.read(source, 16384).toString()));
}
export function credentialReadiness(plan: RepositoryAccessPlan, material: GithubMaterial | null, now: number) {
    if (plan.mode === 'public-readonly')
        return { state: 'public-readonly', privateRead: false, write: false };
    if (!material || enrollmentDigest(material.repositories) !== enrollmentDigest(plan.repositories))
        return { state: 'unavailable', privateRead: false, write: false };
    if (material.expiresAt <= now)
        return { state: 'expired', privateRead: false, write: false };
    return { state: 'material-present-unverified', privateRead: false, write: false };
}
/** No controller auth, OAuth home or conflicting token/config environment reaches gh. */
export function githubChildEnvironment(material: GithubMaterial | null, inherited: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
    const env: NodeJS.ProcessEnv = Object.fromEntries(Object.entries(inherited).filter(([k]) => !k.startsWith('GH_') && !k.startsWith('GITHUB_') && !k.startsWith('GIT_') && !/TOKEN|BEARER|SECRET|PASSWORD|PRIVATE_KEY/i.test(k)));
    if (!env.HOME || !env.HOME.startsWith('/'))
        reject();
    env.GH_CONFIG_DIR = '/etc/openrig-elastic/gh-empty';
    env.GH_HOST = 'github.com';
    env.GH_PROMPT_DISABLED = '1';
    env.GIT_TERMINAL_PROMPT = '0';
    if (material) {
        if (material.expiresAt <= Date.now())
            reject();
        env.GH_TOKEN = material.token;
    }
    return env;
}
/** Pinned launcher invokes this canonical entry; credential is reread on every command. */
export async function runGithubCredentialLauncher(argv: string[]): Promise<number> {
    const store = new GithubCredentialStore();
    const env = githubChildEnvironment(store.read(), process.env);
    return new Promise(resolve => {
        const child = spawn('/usr/bin/gh', argv, { env, stdio: 'inherit' });
        child.on('error', () => resolve(1));
        child.on('exit', code => resolve(code ?? 1));
    });
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    try {
        process.exitCode = await runGithubCredentialLauncher(process.argv.slice(2));
    }
    catch {
        process.stderr.write('elastic_github_credential_unavailable\n');
        process.exitCode = 1;
    }
}
