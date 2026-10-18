import { Hono } from 'hono';
import { z } from 'zod';
import type { ElasticFleetService } from '../domain/elastic-fleet.js';
import { authBearerTokenMiddleware } from '../middleware/auth-bearer-token.js';
export function elasticFleetRoutes(options: {
    service?: ElasticFleetService;
    bearerToken: string | null;
}) {
    const app = new Hono();
    app.use('*', async (c, next) => { if (!options.bearerToken)
        return c.json({ error: 'elastic_operator_auth_required' }, 503); await next(); });
    app.use('*', authBearerTokenMiddleware({ expectedToken: options.bearerToken }));
    app.use('*', async (c, next) => { if (!options.service)
        return c.json({ error: 'elastic_fleet_unavailable' }, 503); await next(); });
    app.get('/', c => c.json(options.service!.status()));
    app.get('/plan', async (c) => { try {
        return c.json(await options.service!.plan());
    }
    catch {
        return c.json({ error: 'observation_failed' }, 503);
    } });
    app.post('/configure', async (c) => { try {
        return c.json(options.service!.configure(await c.req.json()));
    }
    catch {
        return c.json({ error: 'invalid_configuration' }, 400);
    } });
    app.post('/pause', c => c.json(options.service!.pause()));
    app.post('/drain', async (c) => { try {
        const input = z.object({ workerId: z.string().min(1).max(512) }).strict().parse(await c.req.json());
        return c.json(options.service!.requestDrain(input.workerId));
    }
    catch {
        return c.json({ error: 'invalid_drain' }, 400);
    } });
    app.post('/reconcile', async (c) => { try {
        const result = await options.service!.reconcile();
        return c.json(result, result.error ? 503 : 200);
    }
    catch {
        return c.json({ error: 'elastic_reconcile_unavailable' }, 503);
    } });
    return app;
}
