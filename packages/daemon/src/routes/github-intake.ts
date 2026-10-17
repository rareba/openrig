import { Hono } from "hono";
import type { GithubIntakeService, GithubIntakeConfig, GithubEligibilityRequest } from "../domain/github-intake.js";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";

export function githubIntakeRoutes(options: { service?: GithubIntakeService; bearerToken: string | null }) {
  const app = new Hono();
  const auth = authBearerTokenMiddleware({ expectedToken: options.bearerToken });
  app.post("*", auth);
  app.use("*", async (c, next) => {
    if (!options.service) return c.json({ error: "github_intake_unavailable" }, 503);
    await next();
  });
  app.get("/", c => c.json(options.service!.status()));
  app.post("/configure", async c => {
    try { return c.json(options.service!.configure(await c.req.json<GithubIntakeConfig>())); }
    catch (error) { return c.json({ error: error instanceof Error ? error.message : String(error) }, 400); }
  });
  app.post("/eligibility", async c => {
    try { return c.json(options.service!.markEligibility(await c.req.json<GithubEligibilityRequest>())); }
    catch (error) { return c.json({ error: error instanceof Error ? error.message : String(error) }, 400); }
  });
  app.post("/reconcile", async c => {
    const result = await options.service!.reconcile();
    return c.json(result, result.error ? 503 : 200);
  });
  return app;
}
