import { describe, expect, it } from 'vitest';
import { Hono } from 'hono';
import { elasticWorkerAuth } from '../src/middleware/elastic-worker-auth.js';
describe('elastic worker API boundary', () => {
    function app(enabled: boolean, token: string | null = 'worker-token') { const a = new Hono(); a.use('/api/*', elasticWorkerAuth({ enabled, bearerToken: token })); a.all('/api/*', c => c.json({ accepted: true })); return a; }
    it('requires worker auth for all ordinary sensitive paths only on elastic hosts', async () => {
        for (const path of ['/api/bootstrap/apply', '/api/queue', '/api/rigs/import', '/api/activity/identity']) {
            expect((await app(true).request(path, { method: 'POST' })).status).toBe(401);
            expect((await app(true).request(path, { method: 'POST', headers: { Authorization: 'Bearer worker-token' } })).status).toBe(200);
            expect((await app(false).request(path, { method: 'POST' })).status).toBe(200);
        }
        expect((await app(true, null).request('/api/bootstrap')).status).toBe(503);
    });
    it('defers only exact existing activity-token POST checks', async () => {
        const a = app(true);
        for (const path of ['/api/activity/hooks', '/api/activity/pi-usage', '/api/activity/pi-usage-charges']) {
            expect((await a.request(path, { method: 'POST' })).status).toBe(200);
            expect((await a.request(path)).status).toBe(401);
        }
        expect((await a.request('/api/activity/hooks/extra', { method: 'POST' })).status).toBe(401);
    });
});
import Database from 'better-sqlite3';
import { migrate } from '../src/db/migrate.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { createTestApp } from './helpers/test-app.js';
it('applies the gate to real native routes and preserves the activity hook token boundary', async () => {
    const db = new Database(':memory:');
    migrate(db, ALL_MIGRATIONS);
    try {
        const { app } = createTestApp(db, { activityHookToken: 'activity-token', appDeps: { missionControlBearerToken: 'worker-token', elasticWorkerProbe: () => { throw Error('fixture-unavailable'); } } });
        for (const [path, method] of [['/api/bootstrap/plan', 'POST'], ['/api/queue', 'GET'], ['/api/rigs/import', 'POST'], ['/api/whoami', 'GET']]) {
            expect((await app.request(path!, { method })).status).toBe(401);
            expect((await app.request(path!, { method, headers: { Authorization: 'Bearer worker-token', 'Content-Type': 'application/json' }, ...(method === 'POST' ? { body: '{}' } : {}) })).status).not.toBe(401);
        }
        for (const path of ['/api/activity/hooks', '/api/activity/pi-usage', '/api/activity/pi-usage-charges']) {
            expect((await app.request(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).status).toBe(401);
            expect((await app.request(path, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer worker-token' }, body: '{}' })).status).toBe(401);
            expect((await app.request(path, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-openrig-activity-token': 'activity-token' }, body: '{}' })).status).not.toBe(401);
        }
    }
    finally {
        db.close();
    }
});
