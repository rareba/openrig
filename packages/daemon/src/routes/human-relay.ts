import { Hono } from "hono";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { getSelfHostId, resolvesToLocalHost } from "../domain/hosts/fanout-contract.js";
import { loadHostRegistry, resolveHost } from "../domain/hosts/hosts-registry-reader.js";
import { remoteJsonRequest } from "../domain/hosts/remote-daemon-http.js";
import { QueueRepositoryError, type QueueRepository } from "../domain/queue-repository.js";
import type { HumanRelayService } from "../domain/human-relay.js";
import { humanResponsesRoutes } from "./human-responses.js";
/** Trusted connector surface; bindings derive the native human, never an LLM/body actor claim. */
export function humanRelayRoutes(opts: {
    service?: HumanRelayService;
    queueRepo?: QueueRepository;
    bearerToken?: string | null;
}): Hono {
    const app = new Hono();
    const native = humanResponsesRoutes(opts);
    for (const operation of ["pull", "webhook-pull", "webhook-accepted", "receipt", "validate", "context", "respond"] as const) {
        app.post(`/human-relay/${operation}`, authBearerTokenMiddleware({ expectedToken: opts.bearerToken ?? null }), async (c) => {
            let body: Record<string, unknown>;
            try {
                const raw: unknown = await c.req.json();
                if (!raw || typeof raw !== "object" || Array.isArray(raw))
                    throw Error();
                body = raw as Record<string, unknown>;
            }
            catch {
                return c.json({ error: "invalid_body" }, 400);
            }
            if (typeof body.hostId === "string" && body.hostId && !resolvesToLocalHost(body.hostId, getSelfHostId())) {
                const loader = (c.get("hostRegistryLoader" as never) as typeof loadHostRegistry | undefined) ?? loadHostRegistry;
                const registry = loader();
                if (!registry.ok)
                    return c.json({ error: "relay_registry_unavailable" }, 502);
                const target = resolveHost(registry.registry, body.hostId);
                if (!target.ok || target.host.transport !== "http")
                    return c.json({ error: "relay_host_unavailable" }, 502);
                const { hostId: _, ...forward } = body;
                const result = await remoteJsonRequest(target.host, `/api/mission-control/human-relay/${operation}`, { method: "POST", timeoutMs: 15000, body: forward, fetchImpl: c.get("remoteFetchImpl" as never) as typeof fetch | undefined, headers: { "X-OpenRig-Relay": getSelfHostId() ?? "unknown" } });
                if (result.ok)
                    return c.json(result.payload as Record<string, unknown>);
                // Keep terminal source refusals terminal. A stale answer must not retry as a transport failure.
                if (result.kind === "http" && result.status && [400, 401, 403, 404, 409].includes(result.status))
                    return c.json({ error: result.detail || "relay_refused" }, result.status as 400 | 401 | 403 | 404 | 409);
                return c.json({ error: "relay_origin_unavailable" }, 502);
            }
            if (!opts.service)
                return c.json({ error: "human_relay_unavailable" }, 503);
            try {
                if (operation === "pull")
                    return c.json(opts.service.pull(body));
                if (operation === "webhook-pull")
                    return c.json(opts.service.webhookPull(body));
                if (operation === "webhook-accepted")
                    return c.json(opts.service.webhookAccepted(body));
                if (operation === "receipt")
                    return c.json(opts.service.receipt(body));
                if (operation === "validate")
                    return c.json(opts.service.validate(body));
                if (operation === "context")
                    return c.json(opts.service.context(body));
                const input = opts.service.responseInput(body);
                // Reuse the complete native answer/clarification writer, including remote return activation
                // and durable wake intents. This connector owns no approval or execution path.
                return native.request("/human-response", { method: "POST", headers: { "Content-Type": "application/json", ...(opts.bearerToken ? { Authorization: `Bearer ${opts.bearerToken}` } : {}), "X-OpenRig-Session": input.actorSession }, body: JSON.stringify(input) });
            }
            catch (error) {
                if (error instanceof QueueRepositoryError)
                    return c.json({ error: error.code, message: error.message }, error.code === "human_relay_not_found" ? 404 : 409);
                throw error;
            }
        });
    }
    return app;
}
