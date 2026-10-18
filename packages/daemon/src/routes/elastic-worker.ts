import { Hono } from 'hono';
import { authBearerTokenMiddleware } from '../middleware/auth-bearer-token.js';
import type { ElasticWorkerProbe } from '../adapters/elastic-worker-probe.js';
export type ElasticWorkerProbeReader = () => ElasticWorkerProbe | Promise<ElasticWorkerProbe>;
/** Root control-plane mutations deliberately have no daemon HTTP equivalent. */
export function elasticWorkerRoutes(options: { bearerToken: string | null; probe?: ElasticWorkerProbeReader }) {
  const app = new Hono();
  app.use('*', async (c,next) => {
    if (!options.bearerToken) return c.json({ error: 'elastic_worker_unavailable' },503);
    return authBearerTokenMiddleware({ expectedToken: options.bearerToken })(c,next);
  });
  app.get('/probe', async c => {
    if (!options.probe) return c.json({ error: 'elastic_worker_unavailable' },503);
    try { return c.json(await options.probe()); }
    catch { return c.json({ error: 'elastic_worker_unavailable' },503); }
  });
  return app;
}
