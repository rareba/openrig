import { readFileSync } from 'node:fs';
import { Command } from 'commander';
import { DaemonClient } from '../client.js';
import { daemonStatusGuard, getDaemonStatus, getDaemonUrl } from '../daemon-lifecycle.js';
import { realDeps } from './daemon.js';
import type { StatusDeps } from './status.js';
export function elasticCommand(override?: StatusDeps): Command {
    const command = new Command('elastic').description('Inspect and reconcile the opt-in elastic worker pool');
    async function request(method: 'GET' | 'POST', path: string, body: unknown, json?: boolean) {
        const deps = override ?? { lifecycleDeps: realDeps(), clientFactory: (url: string) => new DaemonClient(url) };
        const status = await getDaemonStatus(deps.lifecycleDeps);
        if (!daemonStatusGuard(status, { json }))
            return;
        const client = deps.clientFactory(getDaemonUrl(status));
        const bearer = process.env.OPENRIG_AUTH_BEARER_TOKEN;
        const auth = bearer ? { headers: { Authorization: `Bearer ${bearer}` } } : undefined;
        const result = method === 'GET' ? await client.get(path, auth) : await client.post(path, body, auth);
        console.log(JSON.stringify(result.data, null, json ? undefined : 2));
        if (result.status >= 400)
            process.exitCode = result.status >= 500 ? 2 : 1;
    }
    command.command('configure').description('Validate and save the nonsecret policy and deployment profile pointer')
        .requiredOption('--file <path>', 'JSON configuration file').option('--json', 'JSON output')
        .action((o: {
        file: string;
        json?: boolean;
    }) => request('POST', '/api/elastic-fleet/configure', JSON.parse(readFileSync(o.file, 'utf8')), o.json));
    for (const [name, path] of [['status', ''], ['plan', '/plan']] as const)
        command.command(name).option('--json', 'JSON output').action((o: {
            json?: boolean;
        }) => request('GET', `/api/elastic-fleet${path}`, undefined, o.json));
    for (const [name, path] of [['sync', 'reconcile'], ['pause', 'pause']] as const)
        command.command(name).option('--json', 'JSON output').action((o: {
            json?: boolean;
        }) => request('POST', `/api/elastic-fleet/${path}`, {}, o.json));
    command.command('drain <worker-id>').description('Exclude an exact worker from new root admission').option('--json', 'JSON output').action((workerId: string, o: {
        json?: boolean;
    }) => request('POST', '/api/elastic-fleet/drain', { workerId }, o.json));
    return command;
}
