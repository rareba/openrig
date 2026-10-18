import { createHash } from "node:crypto";
import { Hono } from "hono";
import { loadHostRegistry, resolveHost } from "../domain/hosts/hosts-registry-reader.js";
import { getSelfHostId, resolvesToLocalHost } from "../domain/hosts/fanout-contract.js";
import { remoteJsonRequest } from "../domain/hosts/remote-daemon-http.js";
import { QueueRepositoryError, stampSelfHostSuffix, type QueueCreateInput, type QueueItem, type QueueRepository } from "../domain/queue-repository.js";
import { parseHumanResponse } from "../domain/human-questions.js";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { resolveActorWithDeferral, resolveRecordedProvenance } from "./require-sender-identity.js";

/** Local operator surface; the privileged bearer and loopback policy match Mission Control. */
export function humanResponsesRoutes(opts: { queueRepo?: QueueRepository; bearerToken?: string | null }): Hono {
  const app = new Hono();
  app.post("/human-response-return", authBearerTokenMiddleware({ expectedToken: opts.bearerToken ?? null }), async (c) => {
    if (!opts.queueRepo) return c.json({ error: "queue_unavailable", message: "The native queue is unavailable." }, 503);
    let body: Record<string, unknown>;
    try { body = await c.req.json(); } catch { return c.json({ error: "invalid_body" }, 400); }
    if (!body || typeof body !== "object") return c.json({ error: "invalid_body" }, 400);
    const claim = body.reply && typeof body.reply === "object" ? (body.reply as Record<string, unknown>).sourceSession : body.actorSession;
    const actor = resolveActorWithDeferral(c, { verb: "human response return", bodyClaim: typeof claim === "string" ? claim : null });
    if (!actor.ok) return actor.response;
    try {
      if (body.phase === "stage") {
        const raw = body.reply as Record<string, unknown> | undefined;
        if (!raw || typeof raw.qitemId !== "string" || !/^human-reply-[a-f0-9]{64}$/.test(raw.qitemId)
          || typeof raw.destinationSession !== "string" || typeof raw.body !== "string"
          || !Array.isArray(raw.tags) || !raw.tags.every(t => typeof t === "string") || !raw.tags.includes("human-response")
          || !raw.tags.some(t => /^response:[a-f0-9]{64}$/.test(t))
          || !Array.isArray(raw.chainOfRecord) || !raw.chainOfRecord.length || !raw.chainOfRecord.every(id => typeof id === "string")) {
          return c.json({ error: "invalid_human_response", message: "Provide the native response ID, destination, content, fingerprint and request lineage." }, 400);
        }
        const reply: QueueCreateInput = { qitemId: raw.qitemId, sourceSession: actor.session, destinationSession: raw.destinationSession,
          body: raw.body, tags: raw.tags, chainOfRecord: raw.chainOfRecord, identityProvenance: resolveRecordedProvenance(c, actor) };
        return c.json(opts.queueRepo.stageHumanResponseReturn(reply));
      }
      if (body.phase === "activate" && typeof body.qitemId === "string" && typeof body.fingerprint === "string" && /^[a-f0-9]{64}$/.test(body.fingerprint)) {
        return c.json(opts.queueRepo.activateHumanResponseReturn({ qitemId: body.qitemId, fingerprint: body.fingerprint,
          actorSession: actor.session, identityProvenance: resolveRecordedProvenance(c, actor) }));
      }
      return c.json({ error: "invalid_human_response", message: "Provide a stage or activate phase." }, 400);
    } catch (err) {
      if (!(err instanceof QueueRepositoryError)) throw err;
      return c.json({ error: err.code, message: err.message }, 409);
    }
  });
  app.post("/human-response-receipt", authBearerTokenMiddleware({ expectedToken: opts.bearerToken ?? null }), async (c) => {
    if (!opts.queueRepo) return c.json({ error: "queue_unavailable" }, 503);
    let body: Record<string, unknown>;
    try { const raw: unknown = await c.req.json(); if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw Error(); body = raw as Record<string, unknown>; }
    catch { return c.json({ error: "invalid_body" }, 400); }
    const actor = resolveActorWithDeferral(c, { verb: "human response receipt", bodyClaim: typeof body.actorSession === "string" ? body.actorSession : null });
    if (!actor.ok) return actor.response;
    const response = parseHumanResponse(body.response);
    if (typeof body.qitemId !== "string" || typeof body.replyId !== "string" || typeof body.requestRevision !== "string" || !response) return c.json({ error: "invalid_human_response" }, 400);
    try { return c.json(opts.queueRepo.humanResponseCommitEvidence({ qitemId: body.qitemId, replyId: body.replyId,
      requestRevision: body.requestRevision, actorSession: actor.session, identityProvenance: resolveRecordedProvenance(c, actor), response })); }
    catch (error) { if (!(error instanceof QueueRepositoryError)) throw error; return c.json({ error: error.code, message: error.message }, 409); }
  });
  app.post("/human-response", authBearerTokenMiddleware({ expectedToken: opts.bearerToken ?? null }), async (c) => {
    let body: Record<string, unknown>;
    try {
      const parsed: unknown = await c.req.json();
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("object required");
      body = parsed as Record<string, unknown>;
    } catch {
      return c.json({ error: "invalid_body", message: "Provide a JSON human response object." }, 400);
    }
    const actor = resolveActorWithDeferral(c, { verb: "human response", bodyClaim: typeof body.actorSession === "string" ? body.actorSession : null });
    if (!actor.ok) return actor.response;
    const response = parseHumanResponse(body.response);
    if (typeof body.qitemId !== "string" || typeof body.replyId !== "string" || typeof body.requestRevision !== "string" || !response) {
      return c.json({ error: "invalid_human_response", message: "Provide the request qitemId, replyId, and an explicit choice, custom answer, or clarification." }, 400);
    }
    if (typeof body.hostId === "string" && body.hostId && !resolvesToLocalHost(body.hostId, getSelfHostId())) {
      const loader = (c.get("hostRegistryLoader" as never) as (() => ReturnType<typeof loadHostRegistry>) | undefined) ?? loadHostRegistry;
      const registry = loader();
      if (!registry.ok) return c.json({ error: "remote_response_failed", message: registry.error }, 502);
      const target = resolveHost(registry.registry, body.hostId);
      if (!target.ok) return c.json({ error: "remote_response_failed", message: target.error }, 502);
      if (target.host.transport !== "http") return c.json({ error: "remote_response_failed", message: "Responding requires a registered HTTP host." }, 502);
      const result = await remoteJsonRequest(target.host, "/api/mission-control/human-response", {
        method: "POST", timeoutMs: 10_000,
        fetchImpl: c.get("remoteFetchImpl" as never) as typeof fetch | undefined,
        body: { qitemId: body.qitemId, replyId: body.replyId, requestRevision: body.requestRevision, response },
        headers: { "X-OpenRig-Session": actor.session, "X-OpenRig-Relay": getSelfHostId() ?? "unknown", "X-OpenRig-Provenance": actor.provenance },
      });
      if (result.ok) return c.json(result.payload as Record<string, unknown>);
      return c.json({ error: "remote_response_failed", message: result.detail, failureClass: result.kind }, 502);
    }
    if (!opts.queueRepo) return c.json({ error: "queue_unavailable", message: "The native queue is unavailable." }, 503);
    try {
      const input = { qitemId: body.qitemId, replyId: body.replyId,
        requestRevision: body.requestRevision, actorSession: actor.session, identityProvenance: resolveRecordedProvenance(c, actor), response };
      const plan = opts.queueRepo.prepareHumanResponse(input);
      let remoteReply: QueueItem | undefined;
      let activate: (() => Promise<Response | null>) | undefined;
      if (plan.receipt.replyHostId) {
        const loader = (c.get("hostRegistryLoader" as never) as (() => ReturnType<typeof loadHostRegistry>) | undefined) ?? loadHostRegistry;
        const registry = loader();
        if (!registry.ok) return c.json({ error: "remote_response_failed", message: registry.error }, 502);
        const target = resolveHost(registry.registry, plan.receipt.replyHostId);
        if (!target.ok) return c.json({ error: "remote_response_failed", message: target.error }, 502);
        if (target.host.transport !== "http") return c.json({ error: "remote_response_failed", message: "The original manager requires a registered HTTP host." }, 502);
        const remoteSource = stampSelfHostSuffix(actor.session);
        const remoteHost = target.host;
        const send = (payload: unknown) => remoteJsonRequest(remoteHost, "/api/mission-control/human-response-return", {
          method: "POST", timeoutMs: 10_000,
          fetchImpl: c.get("remoteFetchImpl" as never) as typeof fetch | undefined,
          body: payload,
          headers: { "X-OpenRig-Session": remoteSource, "X-OpenRig-Relay": getSelfHostId() ?? "unknown", "X-OpenRig-Provenance": actor.provenance },
        });
        if (plan.status === "prepared") {
          const staged = await send({ phase: "stage", reply: { ...plan.reply, sourceSession: remoteSource } });
          if (!staged.ok) return c.json({ error: "remote_response_failed", message: staged.detail, failureClass: staged.kind }, 502);
          remoteReply = staged.payload as QueueItem;
        }
        activate = async () => {
          const activated = await send({ phase: "activate", qitemId: plan.receipt.replyQitemId,
            fingerprint: createHash("sha256").update(JSON.stringify([input.requestRevision, input.response])).digest("hex") });
          if (!activated.ok) return c.json({ error: "remote_response_failed", message: activated.detail, failureClass: activated.kind, receipt: plan.receipt }, 502);
          const item = activated.payload as QueueItem;
          if (item.qitemId !== plan.receipt.replyQitemId || !item.tags?.includes("human-response-activated")) {
            return c.json({ error: "remote_response_failed", message: "The target did not confirm activation of the committed response." }, 502);
          }
          opts.queueRepo!.acknowledgeHumanResponseActivation(plan.receipt.replyQitemId);
          return null;
        };
      }
      const receipt = plan.status === "recorded" ? plan.receipt : opts.queueRepo.recordHumanResponse(input, remoteReply);
      const failure = await activate?.();
      return failure ?? c.json(receipt);
    } catch (err) {
      if (!(err instanceof QueueRepositoryError)) throw err;
      return c.json({ error: err.code, message: err.message }, err.code === "invalid_human_response" ? 400 : 409);
    }
  });
  return app;
}
