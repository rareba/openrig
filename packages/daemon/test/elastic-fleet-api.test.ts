import { afterEach, expect, it } from 'vitest';
import { Hono } from 'hono';
import { createFullTestDb } from './helpers/test-app.js';
import { migrate } from '../src/db/migrate.js';
import { elasticFleetSchema } from '../src/db/migrations/116_elastic_fleet.js';
import { ElasticFleetService, type FleetIO } from '../src/domain/elastic-fleet.js';
import { elasticFleetRoutes } from '../src/routes/elastic-fleet.js';
const cleanups: Array<() => void> = [];
afterEach(() => { for (const close of cleanups.splice(0))
    close(); });
it('protects all operator routes and keeps status and plan free of mutation', async () => {
    const db = createFullTestDb();
    cleanups.push(() => db.close());
    migrate(db, [elasticFleetSchema]);
    let mutations = 0;
    const io: FleetIO = { observe: async () => ({ observedAt: 10000, now: 10000, demands: [], workers: [], providerAvailable: false }), provision: async () => { mutations++; }, beginDrain: async () => { mutations++; }, remove: async () => { mutations++; }, inspectOperation: async () => { throw Error('unexpected'); }, advanceOperation: async () => { mutations++; return 'pending'; } };
    const service = new ElasticFleetService(db, io, () => 10000);
    const app = new Hono().route('/api/elastic-fleet', elasticFleetRoutes({ service, bearerToken: 'operator' }));
    for (const path of ['', '/plan'])
        expect((await app.request(`/api/elastic-fleet${path}`)).status).toBe(401);
    for (const path of ['/configure', '/pause', '/drain', '/reconcile'])
        expect((await app.request(`/api/elastic-fleet${path}`, { method: 'POST', body: '{}' })).status).toBe(401);
    const unavailable = new Hono().route('/', elasticFleetRoutes({ bearerToken: 'operator' }));
    expect((await unavailable.request('/', { headers: { Authorization: 'Bearer operator' } })).status).toBe(503);
    const headers = { Authorization: 'Bearer operator', 'content-type': 'application/json' };
    const configuration = { policy: { poolId: 'test', enabled: false, mode: 'dry-run', maxWorkers: 1, slotsPerWorker: 2, capabilities: ['code'], pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900 } };
    expect((await app.request('/api/elastic-fleet/configure', { method: 'POST', headers, body: JSON.stringify(configuration) })).status).toBe(200);
    const before = db.serialize();
    for (const path of ['', '/plan'])
        expect((await app.request(`/api/elastic-fleet${path}`, { headers })).status).toBe(200);
    expect(db.serialize()).toEqual(before);
    expect(mutations).toBe(0);
    expect((await app.request('/api/elastic-fleet/drain', { method: 'POST', headers, body: JSON.stringify({ workerId: 'worker', assumeClean: true }) })).status).toBe(400);
    expect((await app.request('/api/elastic-fleet/reconcile', { method: 'POST', headers })).status).toBe(200);
    expect(mutations).toBe(0);
    const noToken = new Hono().route('/', elasticFleetRoutes({ service, bearerToken: null }));
    expect((await noToken.request('/')).status).toBe(503);
});
