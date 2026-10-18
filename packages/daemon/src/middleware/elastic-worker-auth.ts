import type { MiddlewareHandler } from 'hono';
import { constantTimeEqual } from './auth-bearer-token.js';
const activityPosts = new Set(['/api/activity/hooks', '/api/activity/pi-usage', '/api/activity/pi-usage-charges']);
/** These three routes still run their mandatory seat-scoped activity-token and provenance checks. */
export function elasticWorkerAuth(options: {
    enabled: boolean;
    bearerToken: string | null;
}): MiddlewareHandler {
    return async (c, next) => {
        if (!options.enabled)
            return next();
        if (!options.bearerToken)
            return c.json({ error: 'elastic_worker_unavailable' }, 503);
        if (c.req.method === 'POST' && activityPosts.has(c.req.path))
            return next();
        const token = /^Bearer ([^\s]+)$/.exec(c.req.header('Authorization') ?? '')?.[1];
        if (!token || !constantTimeEqual(token, options.bearerToken))
            return c.json({ error: 'unauthorized' }, 401);
        return next();
    };
}
