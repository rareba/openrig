import { afterEach, expect, it, vi } from 'vitest';
import { createServer } from 'node:http';
import { once } from 'node:events';
import * as daemon from '../src/commands/daemon.js';
import { createProgram } from '../src/index.js';
import { createLazyProgram } from '../src/lazy-program.js';
import { STATE_FILE, type LifecycleDeps } from '../src/daemon-lifecycle.js';
const cleanup: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const close of cleanup.splice(0))
    await close(); vi.unstubAllEnvs(); vi.restoreAllMocks(); process.exitCode = 0; });
it('registers identical eager and lazy elastic commands', async () => {
    const eager = createProgram().commands.find(c => c.name() === 'elastic');
    const lazy = (await createLazyProgram('elastic'))!.commands.find(c => c.name() === 'elastic');
    expect(eager!.commands.map(c => c.name())).toEqual(['configure', 'status', 'plan', 'sync', 'pause', 'drain']);
    expect(lazy!.commands.map(c => c.name())).toEqual(eager!.commands.map(c => c.name()));
});
it('runs the real lazy command and DaemonClient through HTTP with operator auth and error exit', async () => {
    const requests: Array<{
        url: string;
        auth: string | undefined;
    }> = [];
    const server = createServer((req, res) => { requests.push({ url: req.url!, auth: req.headers.authorization }); res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'elastic_reconcile_unavailable' })); });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    cleanup.push(() => new Promise<void>(resolve => server.close(() => resolve())));
    const address = server.address();
    if (!address || typeof address === 'string')
        throw Error('fixture');
    const lifecycleDeps = { exists: (p: string) => p === STATE_FILE, readFile: () => JSON.stringify({ pid: 123, port: address.port, db: 'test.sqlite', startedAt: '2026-10-18T00:00:00Z' }), isProcessAlive: () => true, fetch: async () => new Response(JSON.stringify({ status: 'ok' }), { status: 200 }) } as LifecycleDeps;
    vi.spyOn(daemon, 'realDeps').mockReturnValue(lifecycleDeps as ReturnType<typeof daemon.realDeps>);
    vi.stubEnv('OPENRIG_AUTH_BEARER_TOKEN', 'fixture-operator');
    vi.spyOn(console, 'log').mockImplementation(() => { });
    await (await createLazyProgram('elastic'))!.parseAsync(['elastic', 'sync', '--json'], { from: 'user' });
    expect(requests).toEqual([{ url: '/api/elastic-fleet/reconcile', auth: 'Bearer fixture-operator' }]);
    expect(process.exitCode).toBe(2);
});
