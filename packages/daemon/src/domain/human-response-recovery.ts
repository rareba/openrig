import { loadHostRegistry, resolveHost } from "./hosts/hosts-registry-reader.js";
import { getSelfHostId } from "./hosts/fanout-contract.js";
import { remoteJsonRequest } from "./hosts/remote-daemon-http.js";
import type { QueueRepository } from "./queue-repository.js";

/** Bounded native outbox recovery, called at startup and by the existing periodic sweep.
 * Activation is idempotent at the target; uncertain replies remain pending for the next pass.
 */
export async function recoverHumanResponseReturns(repo: QueueRepository, deps: {
  loadRegistry?: typeof loadHostRegistry;
  fetchImpl?: typeof fetch;
} = {}): Promise<{ activated: number; pending: number; errors: string[] }> {
  const intents = repo.pendingHumanResponseActivations(4);
  const result = { activated: 0, pending: 0, errors: [] as string[] };
  if (!intents.length) return result;
  const registry = (deps.loadRegistry ?? loadHostRegistry)();
  for (const intent of intents) {
    try {
      const saved: unknown = JSON.parse(intent.body);
      if (!saved || typeof saved !== "object") throw new Error("Invalid human response activation intent.");
      const p = saved as Record<string, unknown>;
      if (typeof p.replyQitemId !== "string" || typeof p.replyHostId !== "string" || typeof p.actorSession !== "string"
        || typeof p.fingerprint !== "string" || !/^[a-f0-9]{64}$/.test(p.fingerprint)) throw new Error("Incomplete human response activation intent.");
      if (!registry.ok) throw new Error(registry.error);
      const target = resolveHost(registry.registry, p.replyHostId);
      if (!target.ok) throw new Error(target.error);
      if (target.host.transport !== "http") throw new Error("Human response activation requires a registered HTTP host.");
      const response = await remoteJsonRequest(target.host, "/api/mission-control/human-response-return", {
        method: "POST", timeoutMs: 10_000, fetchImpl: deps.fetchImpl,
        body: { phase: "activate", qitemId: p.replyQitemId, fingerprint: p.fingerprint },
        headers: { "X-OpenRig-Session": p.actorSession, "X-OpenRig-Relay": getSelfHostId() ?? "unknown", "X-OpenRig-Provenance": typeof p.identityProvenance === "string" ? p.identityProvenance : "claimed:v1" },
      });
      if (!response.ok) throw new Error(response.detail);
      const item = response.payload as { qitemId?: string; tags?: string[] };
      if (item.qitemId !== p.replyQitemId || !item.tags?.includes("human-response-activated")) throw new Error("Target did not confirm human response activation.");
      repo.acknowledgeHumanResponseActivation(p.replyQitemId);
      result.activated++;
    } catch (err) {
      repo.deferHumanResponseActivation(intent.outboxId);
      result.pending++;
      result.errors.push(`${intent.outboxId}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  return result;
}
