import { describe, expect, it } from 'vitest';
import { preparationPlanSchema, readinessTaskBody, inspectPiToolProof, readinessProofArtifact } from '../src/adapters/elastic-worker-prepare.js';
const digest = 'a'.repeat(64);
describe('finite native preparation', () => {
    it('rejects arbitrary URLs, missing immutable revisions and nonfinite seats', () => {
        const plan = { version: 1, operationId: 'fixture', manifestDigest: digest, specDigest: digest, model: 'fixture/model', approvedActionKeys: [], repositoryAccess: { mode: 'public-readonly' }, seats: [{ sessionName: 'worker-a@elastic', repository: { id: 1, name: 'fixture/repo', baseRef: 'refs/heads/main', sha: 'a'.repeat(40) } }], owner: null };
        expect(preparationPlanSchema.parse(plan).seats).toHaveLength(1);
        expect(() => preparationPlanSchema.parse({ ...plan, seats: [{ ...plan.seats[0], repository: { ...plan.seats[0]!.repository, url: 'https://evil/' } }] })).toThrow();
        expect(() => preparationPlanSchema.parse({ ...plan, seats: Array(5).fill(plan.seats[0]) })).toThrow();
    });
    it('requires actual current Pi Bash/result/nonce evidence, never a boolean or health receipt', () => {
        const task = { qitemId: 'fixture-task', nonce: 'a'.repeat(32), sourceSession: 'manager@fixture', originHostId: 'fixture-origin' };
        const root = realpathSync(mkdtempSync(join(tmpdir(), 'prepare-proof-')));
        cleanup.push(() => rmSync(root, { recursive: true, force: true }));
        const cwd = join(root, 'repo');
        mkdirSync(cwd);
        const command = readinessTaskBody(task, 'worker-a@elastic', cwd, digest);
        expect(command).toContain('rig whoami');
        const artifact = readinessProofArtifact(task, 'worker-a@elastic', cwd, digest);
        const checksum = join(root, 'checksum.mjs');
        writeFileSync(checksum, `import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
const path = process.argv[2];
process.stdout.write(createHash('sha256').update(readFileSync(path)).digest('hex') + '  ' + path + '\\n');
`);
        // Execute the complete generated command. Only native CLI and Linux checksum transport
        // are local fixtures; the proof writer and marker run unchanged in Bash.
        const output = execFileSync('/bin/bash', ['-c', `rig() { printf '%s\\n' '{}'; }
function /usr/bin/sha256sum() { "$FIXTURE_NODE" "$FIXTURE_CHECKSUM" "$@"; }
${command}`], { encoding: 'utf8', env: { ...process.env, FIXTURE_NODE: process.execPath, FIXTURE_CHECKSUM: checksum } });
        const messages = [{ type: 'message', message: { role: 'assistant', model: 'fixture/model', content: [{ type: 'toolCall', id: 'call', name: 'bash', arguments: { command } }] } }, { type: 'message', message: { role: 'toolResult', toolCallId: 'call', toolName: 'bash', isError: false, content: [{ type: 'text', text: output }] } }];
        expect(readFileSync(artifact.path)).toEqual(Buffer.from(artifact.bytes));
        expect(inspectPiToolProof(messages, { model: 'fixture/model', task, seat: 'worker-a@elastic', cwd, manifestDigest: digest })).toBe(true);
        expect(inspectPiToolProof([{ ready: true }], { model: 'fixture/model', task, seat: 'worker-a@elastic', cwd, manifestDigest: digest })).toBe(false);
        expect(inspectPiToolProof(messages, { model: 'wrong/model', task, seat: 'worker-a@elastic', cwd, manifestDigest: digest })).toBe(false);
    });
});
import Database from 'better-sqlite3';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, realpathSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { execFileSync } from 'node:child_process';
import { afterEach, vi } from 'vitest';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { createTestApp } from './helpers/test-app.js';
import { ElasticWorkerControl } from '../src/domain/elastic-worker-control.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { ElasticWorkerPreparation } from '../src/adapters/elastic-worker-prepare.js';
import { enrollmentDigest } from '../src/adapters/elastic-worker-attempts.js';
import { sha256 } from '../src/adapters/elastic-worker-checkpoint.js';
import type { RuntimeAdapter } from '../src/domain/runtime-adapter.js';
vi.mock('node:child_process', async (importOriginal) => {
    const actual = await importOriginal<typeof import('node:child_process')>();
    return { ...actual, execSync: (command: string, options: unknown) => {
            if (command === 'pi --version')
                return '1.0.4';
            throw Error('fixture_disallows_ambient_shell_probe');
        } };
});
const cleanup: Array<() => void> = [];
afterEach(() => { vi.unstubAllEnvs(); for (const f of cleanup.splice(0))
    f(); });
it('prepares real full Git clones and native SQLite finite seats, replays without a second rig and retains missing tool proof', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'prepare-native-')));
    cleanup.push(() => rmSync(root, { recursive: true, force: true }));
    const dbPath = join(root, 'openrig.db');
    const db = new Database(dbPath);
    cleanup.unshift(() => db.close());
    migrate(db, ALL_MIGRATIONS);
    const source = join(root, 'source');
    mkdirSync(source);
    const env = { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null' };
    execFileSync('/usr/bin/git', ['init', '-b', 'main', source], { env, stdio: 'pipe' });
    writeFileSync(join(source, 'README.md'), 'fixture');
    execFileSync('/usr/bin/git', ['-C', source, 'add', '.'], { env });
    execFileSync('/usr/bin/git', ['-C', source, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-m', 'fixture'], { env, stdio: 'pipe' });
    const head = execFileSync('/usr/bin/git', ['-C', source, 'rev-parse', 'HEAD'], { env, encoding: 'utf8' }).trim();
    const code = join(root, 'runtime');
    const templateDirectory = join(code, 'integrations/azure-elastic');
    mkdirSync(templateDirectory, { recursive: true });
    const actualCode = resolve(import.meta.dirname, '../../..');
    const template = readFileSync(join(actualCode, 'integrations/azure-elastic/worker.rig.yaml'), 'utf8').replaceAll('path:/opt/openrig/integrations/azure-elastic/worker-agent', `path:${actualCode}/integrations/azure-elastic/worker-agent`);
    writeFileSync(join(templateDirectory, 'worker.rig.yaml'), template);
    const m = JSON.parse(readFileSync(join(actualCode, 'integrations/azure-elastic/manifest.example.json'), 'utf8'));
    m.uid = process.getuid!();
    m.gid = process.getgid!();
    m.ownedRoots = [root];
    m.linux.home = join(root, 'home');
    m.linux.sourceRoot = code;
    m.linux.database = dbPath;
    m.linux.temp = join(root, 'tmp');
    m.linux.stateRoot = join(m.linux.home, 'state/pi');
    mkdirSync(m.linux.home);
    mkdirSync(m.linux.temp);
    vi.stubEnv('OPENRIG_DB', dbPath);
    const runtime: RuntimeAdapter = { runtime: 'pi', listInstalled: async () => [], project: async () => ({ projected: [], skipped: [], failed: [] }), deliverStartup: async () => ({ delivered: 0, failed: [] }), checkReady: async () => ({ ready: true }), launchHarness: async () => ({ ok: true }) };
    const setup = createTestApp(db, { adapters: { pi: runtime }, podInstantiatorFsOps: { exists: existsSync, readFile: p => readFileSync(p, 'utf8') } });
    new ElasticWorkerControl(db).enroll({ workerId: 'fixture-worker', vmId: '11111111-1111-4111-8111-111111111111', manifestDigest: digest });
    const plan = preparationPlanSchema.parse({ version: 1, operationId: 'fixture', manifestDigest: digest, specDigest: sha256(Buffer.from(template)), model: 'fixture/model', approvedActionKeys: [], repositoryAccess: { mode: 'public-readonly' }, seats: ['worker-a@elastic', 'worker-b@elastic'].map(sessionName => ({ sessionName, repository: { id: 1, name: 'fixture/repo', baseRef: 'refs/heads/main', sha: head } })), owner: null });
    let clones = 0;
    const service = new ElasticWorkerPreparation({ manifest: m, manifestDigest: digest, approvedPlan: plan, services: { bootstrap: setup.bootstrapOrchestrator, rigs: setup.rigRepo, sessions: setup.sessionRegistry, queue: new QueueRepository(db, setup.eventBus) }, githubFetch: async () => new Response(JSON.stringify({ id: 1, full_name: 'fixture/repo', private: false })), run: async (program, args, cwd, childEnv) => {
            const argv = [...args];
            if (argv.includes('clone')) {
                clones++;
                argv[argv.indexOf('--') + 1] = source;
                const out = execFileSync(program, argv, { cwd, env: { ...childEnv, GIT_ALLOW_PROTOCOL: 'file' }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
                execFileSync('/usr/bin/git', ['-C', argv.at(-1)!, 'remote', 'set-url', 'origin', 'https://github.com/fixture/repo.git'], { env });
                return out;
            }
            if (argv.includes('fetch'))
                argv[argv.indexOf('origin')] = source;
            return execFileSync(program, argv, { cwd, env: { ...childEnv, GIT_ALLOW_PROTOCOL: 'file' }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
        } });
    const request = { operationId: plan.operationId, planDigest: enrollmentDigest(plan) };
    const first = await service.prepare(request);
    expect(first.publicReason).not.toBe('preparation_prerequisite_unavailable');
    expect(setup.rigRepo.findUnarchivedRigsByName('elastic')).toHaveLength(1);
    expect(first.seats.map(s => s.sessionName)).toEqual(['worker-a@elastic', 'worker-b@elastic']);
    expect(first.state).toBe('pending');
    expect(first.seats.every(s => s.toolProven === false)).toBe(true);
    await service.prepare(request);
    expect(clones).toBe(2);
    expect(setup.rigRepo.findUnarchivedRigsByName('elastic')).toHaveLength(1);
});
