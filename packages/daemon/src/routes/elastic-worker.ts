import { z } from 'zod';
import type { ElasticWorkerPreparation } from '../adapters/elastic-worker-prepare.js';
import { Hono } from 'hono';
import { authBearerTokenMiddleware } from '../middleware/auth-bearer-token.js';
import type { ElasticWorkerProbe } from '../adapters/elastic-worker-probe.js';
export type ElasticWorkerProbeReader = () => ElasticWorkerProbe | Promise<ElasticWorkerProbe>;
/** Root control-plane mutations deliberately have no daemon HTTP equivalent. */
export function elasticWorkerRoutes(options: { bearerToken: string | null; probe?: ElasticWorkerProbeReader; preparation?: ElasticWorkerPreparation; runtime?: () => { selfHostId: string | null; build: unknown } }) {
  const app = new Hono();
  app.use('*', async (c,next) => {
    if (!options.bearerToken) return c.json({ error: 'elastic_worker_unavailable' },503);
    return authBearerTokenMiddleware({ expectedToken: options.bearerToken })(c,next);
  });
  app.get('/probe', async c => {
    if (!options.probe) return c.json({ error: 'elastic_worker_unavailable' },503);
    try { return c.json({ ...await options.probe(), ...(options.preparation ? { preparation: options.preparation.inspect() } : {}), ...(options.runtime ? options.runtime() : {}) }); }
    catch { return c.json({ error: 'elastic_worker_unavailable' },503); }
  });
  app.get('/preparation', c => {
    if (!options.preparation) return c.json({ error: 'elastic_worker_unavailable' }, 503);
    try { return c.json(options.preparation.inspect()); } catch { return c.json({ error: 'elastic_worker_unavailable' }, 503); }
  });
  app.post('/prepare', async c => {
    if (!options.preparation) return c.json({ error: 'elastic_worker_unavailable' }, 503);
    const request = z.object({ operationId: z.string().min(1).max(128), planDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict().safeParse(await c.req.json().catch(() => null));
    if (!request.success) return c.json({ error: 'invalid_elastic_preparation' }, 400);
    try { return c.json(await options.preparation.prepare(request.data)); } catch { return c.json({ error: 'elastic_worker_unavailable' }, 503); }
  });
  return app;
}
