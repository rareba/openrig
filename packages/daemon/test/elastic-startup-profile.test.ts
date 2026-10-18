import { readFileSync } from 'node:fs';
import { afterEach, expect, it, vi } from 'vitest';
import { parseLinuxManifest } from '../src/adapters/linux-privileged-worker-boundary.js';
import { preparationPlanSchema } from '../src/adapters/elastic-worker-prepare.js';
import { createDaemon } from '../src/startup.js';

const fixture = vi.hoisted(() => ({
  preparationPath: '/etc/openrig-elastic/preparation.json',
  assertRootPath: vi.fn<typeof import('../src/adapters/linux-privileged-worker-boundary.js').assertRootPath>(),
  noFollowRead: vi.fn<typeof import('../src/adapters/elastic-worker-checkpoint.js').noFollowRead>(),
  loadForegroundManifest: vi.fn<typeof import('../src/adapters/elastic-worker-foreground.js').loadForegroundManifest>(),
  boot: vi.fn(() => { throw Error('default kernel must not start'); }),
}));

vi.mock('../src/domain/kernel-boot.js', () => ({ bootKernelIfNeeded: fixture.boot }));
vi.mock('../src/domain/elastic-worker-guard.js', async original => ({
  ...await original<typeof import('../src/domain/elastic-worker-guard.js')>(),
  assertElasticEffectAllowed: vi.fn(),
}));
vi.mock('../src/adapters/elastic-worker-foreground.js', async original => ({
  ...await original<typeof import('../src/adapters/elastic-worker-foreground.js')>(),
  loadForegroundManifest: fixture.loadForegroundManifest,
}));
vi.mock('../src/adapters/linux-privileged-worker-boundary.js', async original => {
  const actual = await original<typeof import('../src/adapters/linux-privileged-worker-boundary.js')>();
  return {
    ...actual,
    assertRootPath: (path: string) => path === fixture.preparationPath
      ? fixture.assertRootPath(path)
      : actual.assertRootPath(path),
  };
});
vi.mock('../src/adapters/elastic-worker-checkpoint.js', async original => {
  const actual = await original<typeof import('../src/adapters/elastic-worker-checkpoint.js')>();
  return {
    ...actual,
    noFollowRead: (path: string, max?: number) => path === fixture.preparationPath
      ? fixture.noFollowRead(path, max)
      : actual.noFollowRead(path, max),
  };
});

afterEach(() => { vi.unstubAllEnvs(); });

it('composes an opted-in finite Pi worker without default kernel rows or unrelated runtime probes', async () => {
  const digest = 'a'.repeat(64);
  const manifest = parseLinuxManifest(readFileSync(new URL('../../../integrations/azure-elastic/manifest.example.json', import.meta.url), 'utf8')
    .replaceAll('REPLACE_WITH_ROOT_STAGED_TREE_DIGEST', digest)
    .replaceAll('REPLACE_WITH_FILE_DIGEST', digest));
  const plan = preparationPlanSchema.parse({
    version: 1, operationId: 'fixture', manifestDigest: digest, specDigest: digest,
    model: 'fixture/model', approvedActionKeys: [], repositoryAccess: { mode: 'public-readonly' },
    seats: manifest.linux.seats.map(sessionName => ({
      sessionName, repository: { id: 1, name: 'fixture/repo', baseRef: 'refs/heads/main', sha: 'b'.repeat(40) },
    })),
    owner: null,
  });
  fixture.loadForegroundManifest.mockReturnValue(manifest);
  fixture.noFollowRead.mockReturnValue(Buffer.from(JSON.stringify(plan)));
  vi.stubEnv('OPENRIG_ELASTIC_GUARD', '/verified-by-boundary-fixture');
  vi.stubEnv('OPENRIG_ELASTIC_MANIFEST_DIGEST', digest);
  const commands: string[] = [];
  const daemon = await createDaemon({
    dbPath: ':memory:',
    cmuxFactory: async () => { throw Object.assign(Error('absent'), { code: 'ENOENT' }); },
    tmuxExec: async command => { commands.push(command); return ''; },
  });
  try {
    expect(fixture.assertRootPath).toHaveBeenCalledExactlyOnceWith(fixture.preparationPath);
    expect(fixture.noFollowRead).toHaveBeenCalledExactlyOnceWith(fixture.preparationPath, 65536);
    expect(fixture.loadForegroundManifest).toHaveBeenCalledTimes(2);
    expect(fixture.boot).not.toHaveBeenCalled();
    expect(daemon.db.prepare('SELECT * FROM nodes').all()).toEqual([]);
    expect(commands.some(command => /codex|claude|omp/.test(command))).toBe(false);
    expect(daemon.deps.kernelBootTracker).toBeUndefined();
  } finally {
    daemon.deps.gatewaySubsystem?.stop();
    daemon.contextMonitor.stop();
    daemon.eventLoopMonitor.stop();
    daemon.db.close();
  }
}, 30000);
