import { setSelfHostId } from "../src/domain/hosts/fanout-contract.js";
import { recoverHumanResponseReturns } from "../src/domain/human-response-recovery.js";
import { Hono } from "hono";
import { humanResponsesRoutes } from "../src/routes/human-responses.js";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { eventsSchema } from "../src/db/migrations/003_events.js";
import { queueItemsSchema } from "../src/db/migrations/024_queue_items.js";
import { queueTransitionsSchema } from "../src/db/migrations/025_queue_transitions.js";
import { outboxEntriesSchema } from "../src/db/migrations/027_outbox_entries.js";
import { queueItemSummarySchema } from "../src/db/migrations/044_queue_item_summary.js";
import { queueItemEvidenceRefSchema } from "../src/db/migrations/048_queue_item_evidence_ref.js";
import { humanNotificationIntentSchema } from "../src/db/migrations/081_human_notification_intent.js";
import { humanReplyToSchema } from "../src/db/migrations/090_human_reply_to.js";
import { humanQuestionsSchema } from "../src/db/migrations/091_human_questions.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { HUMAN_RESPONSE_ACTIVATION_PREFIX, QueueRepository } from "../src/domain/queue-repository.js";
import { parseHumanQuestions } from "../src/domain/human-questions.js";
const question = { id: "release", question: "Release now?", options: [
  { id: "yes", label: "Release", recommended: true, consequences: "Customers receive this version." },
  { id: "no", label: "Wait", consequences: "Keep the current version." },
] };
describe("local human responses", () => {
  let db: Database.Database;
  let repo: QueueRepository;
  beforeEach(() => {
    db = createDb();
    migrate(db, [coreSchema, eventsSchema, queueItemsSchema, queueTransitionsSchema, outboxEntriesSchema,
      queueItemSummarySchema, queueItemEvidenceRefSchema, humanNotificationIntentSchema, humanReplyToSchema, humanQuestionsSchema]);
    repo = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
    repo.attachOutbox(new OutboxHandler(db));
  });
  afterEach(() => db.close());
  const ask = (questions = [question]) => repo.create({ sourceSession: "manager@team", destinationSession: "operator@external",
    body: "A missing release authority.", summary: "Release decision", evidenceRef: "https://example.com/evidence", humanIntent: "decision", humanQuestions: questions, nudge: false });
  it("preserves validated option consequences", () => {
    expect(parseHumanQuestions([question])).toEqual({ ok: true, questions: [question] });
    expect(parseHumanQuestions([{ ...question, options: [{ ...question.options[0], consequences: 42 }, question.options[1]] }]).ok).toBe(false);
  });
  it("threads clarification without closing or answering; retries once", async () => {
    const item = await ask();
    const input = { qitemId: item.qitemId, requestRevision: repo.getById(item.qitemId)!.humanRequestRevision!, actorSession: "operator@external", replyId: "clarify-1", response: { kind: "clarification" as const, text: "Which customers?" } };
    const result = repo.recordHumanResponse(input);
    expect(result.complete).toBe(false);
    expect(repo.getById(result.replyQitemId)).toMatchObject({ destinationSession: "manager@team", replyTo: item.qitemId });
    expect(repo.getById(result.replyQitemId)?.body).toContain("Which customers?");
    expect(repo.getById(item.qitemId)).toMatchObject({ state: "pending", humanAnswers: null });
    expect(repo.recordHumanResponse(input)).toEqual(result);
    expect(db.prepare("SELECT count(*) AS n FROM outbox_entries").get()).toEqual({ n: 1 });
  });
  it("keeps incomplete custom answers pending and closes a completed set once", async () => {
    const item = await ask([question, { ...question, id: "timing", question: "When?" }]);
    expect(repo.recordHumanResponse({ qitemId: item.qitemId, requestRevision: repo.getById(item.qitemId)!.humanRequestRevision!, actorSession: "operator@external", replyId: "custom-1", response: { kind: "custom", text: "Release only to the test group." } }).complete).toBe(false);
    expect(repo.getById(item.qitemId)?.state).toBe("pending");
    expect(repo.getById(item.qitemId)?.humanAnswers?.release).toMatchObject({ kind: "typed-reply", text: "Release only to the test group.", unansweredCount: 1 });
    const input = { qitemId: item.qitemId, requestRevision: repo.getById(item.qitemId)!.humanRequestRevision!, actorSession: "operator@external", replyId: "choice-2", response: { kind: "choice" as const, questionId: "timing", optionId: "no" } };
    const final = repo.recordHumanResponse(input);
    expect(final.complete).toBe(true);
    expect(repo.getById(item.qitemId)?.state).toBe("done");
    expect(repo.recordHumanResponse(input)).toEqual(final);
    expect(repo.transitionLog.listForQitem(item.qitemId).filter(t => t.state === "done")).toHaveLength(1);
  });
  it("rejects other actors, reply ID conflicts and stale requests without wakes", async () => {
    const item = await ask();
    const input = { qitemId: item.qitemId, requestRevision: repo.getById(item.qitemId)!.humanRequestRevision!, actorSession: "operator@external", replyId: "one", response: { kind: "clarification" as const, text: "Why?" } };
    expect(() => repo.recordHumanResponse({ ...input, actorSession: "stranger@external" })).toThrow(/asked human/);
    repo.recordHumanResponse(input);
    expect(() => repo.recordHumanResponse({ ...input, response: { kind: "clarification", text: "Changed" } })).toThrow(/replyId/);
    repo.update({ qitemId: item.qitemId, actorSession: "manager@team", state: "canceled" });
    expect(() => repo.recordHumanResponse({ ...input, replyId: "two" })).toThrow(/pending/);
    expect(db.prepare("SELECT count(*) AS n FROM outbox_entries").get()).toEqual({ n: 1 });
  });
  it("rejects a changed pending request and rolls back when a wake cannot be retained", async () => {
    const item = await ask();
    const requestRevision = repo.getById(item.qitemId)!.humanRequestRevision!;
    repo.update({ qitemId: item.qitemId, actorSession: "manager@team", summary: "Changed release authority" });
    expect(() => repo.recordHumanResponse({ qitemId: item.qitemId, requestRevision, actorSession: "operator@external", replyId: "stale", response: { kind: "custom", text: "Yes" } })).toThrow(/changed/);
    const noOutbox = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
    expect(() => noOutbox.recordHumanResponse({ qitemId: item.qitemId, requestRevision: repo.getById(item.qitemId)!.humanRequestRevision!, actorSession: "operator@external", replyId: "rollback", response: { kind: "choice", questionId: "release", optionId: "yes" } })).toThrow();
    expect(repo.getById(item.qitemId)).toMatchObject({ state: "pending", humanAnswers: null });
    expect(db.prepare("SELECT count(*) AS n FROM queue_items").get()).toEqual({ n: 1 });
  });
  it("requires the existing bearer and binds the transport actor over a body claim", async () => {
    const item = await ask();
    const app = new Hono().route("/api/mission-control", humanResponsesRoutes({ queueRepo: repo, bearerToken: "operator-token" }));
    const body = { qitemId: item.qitemId, requestRevision: repo.getById(item.qitemId)!.humanRequestRevision!, replyId: "http", actorSession: "operator@external", response: { kind: "custom", text: "Test users only." } };
    const post = (headers: Record<string, string>) => app.request("/api/mission-control/human-response", { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(body) });
    expect((await post({})).status).toBe(401);
    expect((await post({ Authorization: "Bearer operator-token", "X-OpenRig-Session": "stranger@external" })).status).toBe(409);
    expect(repo.getById(item.qitemId)?.state).toBe("pending");
    expect((await post({ Authorization: "Bearer operator-token" })).status).toBe(200);
    expect(repo.getById(item.qitemId)?.state).toBe("done");
  });
  it("forwards the explicit origin using server credentials, never the selected host or local queue", async () => {
    const app = new Hono();
    const captured: { url?: string; init?: RequestInit } = {};
    app.use("*", async (c, next) => {
      c.set("hostRegistryLoader" as never, (() => ({ ok: true, registry: { hosts: [{ id: "vps-b", transport: "http", url: "http://vps-b:7433" }] } })) as never);
      c.set("remoteFetchImpl" as never, (async (url: unknown, init: RequestInit) => {
        captured.url = String(url); captured.init = init;
        return new Response(JSON.stringify({ replyQitemId: "remote-reply", complete: false }), { status: 200 });
      }) as never);
      await next();
    });
    app.route("/api/mission-control", humanResponsesRoutes({ queueRepo: repo }));
    const result = await app.request("/api/mission-control/human-response", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ hostId: "vps-b", qitemId: "remote-request", requestRevision: "remote-revision", replyId: "reply", actorSession: "operator@external", response: { kind: "clarification", text: "Which users?" } }) });
    expect(result.status).toBe(200);
    expect(await result.json()).toEqual({ replyQitemId: "remote-reply", complete: false });
    expect(captured.url).toBe("http://vps-b:7433/api/mission-control/human-response");
    expect(captured.init?.headers).toMatchObject({ "X-OpenRig-Session": "operator@external", "X-OpenRig-Provenance": "claimed:v1" });
    expect(JSON.parse(String(captured.init?.body))).not.toHaveProperty("hostId");
    expect(JSON.parse(String(captured.init?.body))).not.toHaveProperty("actorSession");
    expect(db.prepare("SELECT count(*) AS n FROM queue_items").get()).toEqual({ n: 0 });
  });

  it("only queues a manager continuation while replacement work keeps its execution owner", async () => {
    const replacement = await repo.create({ sourceSession: "manager@team", destinationSession: "worker@team", body: "Independent replacement work", nudge: false });
    repo.claim({ qitemId: replacement.qitemId, destinationSession: "worker@team" });
    const first = await ask();
    const second = await ask();
    for (const item of [first, second]) {
      const result = repo.recordHumanResponse({ qitemId: item.qitemId, requestRevision: item.humanRequestRevision!, actorSession: "operator@external", replyId: "answer", response: { kind: "custom", text: "Test users only." } });
      expect(repo.getById(result.replyQitemId)).toMatchObject({ state: "pending", destinationSession: "manager@team", claimedAt: null });
    }
    expect(repo.getById(replacement.qitemId)).toMatchObject({ state: "in-progress", destinationSession: "worker@team" });
    expect(db.prepare("SELECT count(*) AS n FROM queue_items WHERE state='in-progress'").get()).toEqual({ n: 1 });
  });

  it("answers local human@kernel decisions without a registered external human or Slack", async () => {
    const localRepo = new QueueRepository(db, new EventBus(db), {
      validateRig: session => session === "human@kernel" || session === "manager@team",
      loadHumanRegistry: () => ({ ok: false, error: "No external humans configured", noHumans: true }),
    });
    localRepo.attachOutbox(new OutboxHandler(db));
    const app = new Hono().route("/api/mission-control", humanResponsesRoutes({ queueRepo: localRepo, bearerToken: "operator-token" }));
    const askLocal = () => localRepo.create({ sourceSession: "manager@team", destinationSession: "human@kernel", body: "Decision needed", summary: "Release authority", evidenceRef: "https://example.com/evidence", humanIntent: "decision", humanQuestions: [question], nudge: false });
    const respond = (item: Awaited<ReturnType<typeof askLocal>>, kind: string, extra: Record<string, string>) => app.request("/api/mission-control/human-response", {
      method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer operator-token" },
      body: JSON.stringify({ qitemId: item.qitemId, requestRevision: localRepo.getById(item.qitemId)!.humanRequestRevision, actorSession: "human@kernel", replyId: kind, response: { kind, ...extra } }),
    });
    const choice = await askLocal();
    expect((await respond(choice, "choice", { questionId: "release", optionId: "yes" })).status).toBe(200);
    expect(localRepo.getById(choice.qitemId)).toMatchObject({ state: "done", humanAnswers: { release: "yes" } });
    const custom = await askLocal();
    expect((await respond(custom, "custom", { text: "Test users only." })).status).toBe(200);
    expect(localRepo.getById(custom.qitemId)).toMatchObject({ state: "done", humanAnswers: { release: { text: "Test users only." } } });
    const clarify = await askLocal();
    const result = await respond(clarify, "clarification", { text: "Which users?" });
    expect(result.status).toBe(200);
    expect(localRepo.getById(clarify.qitemId)).toMatchObject({ state: "pending", humanAnswers: null });
    expect(localRepo.getById((await result.json()).replyQitemId)).toMatchObject({ sourceSession: "human@kernel", destinationSession: "manager@team", state: "pending" });
  });

  it.each(["normal", "stage-loss", "activation-loss", "stale-source"])("returns clarification/custom to a strict remote native queue; case=%s", async (mode) => {
    const remoteDb = createDb();
    migrate(remoteDb, [coreSchema, eventsSchema, queueItemsSchema, queueTransitionsSchema, outboxEntriesSchema,
      queueItemSummarySchema, queueItemEvidenceRefSchema, humanNotificationIntentSchema, humanReplyToSchema, humanQuestionsSchema]);
    const remoteRepo = new QueueRepository(remoteDb, new EventBus(remoteDb), { validateRig: session => session === "worker@team" || session === "manager@team" });
    remoteRepo.attachOutbox(new OutboxHandler(remoteDb));
    const remoteApp = new Hono();
    remoteApp.use("*", async (c, next) => { c.set("queueRepo" as never, remoteRepo as never); await next(); });
    remoteApp.route("/api/mission-control", humanResponsesRoutes({ queueRepo: remoteRepo }));
    const replacement = await remoteRepo.create({ sourceSession: "manager@team", destinationSession: "worker@team", body: "Existing independent execution", nudge: false });
    remoteRepo.claim({ qitemId: replacement.qitemId, destinationSession: "worker@team" });
    let remoteCalls = 0;
    const localApp = new Hono();
    localApp.use("*", async (c, next) => {
      c.set("hostRegistryLoader" as never, (() => ({ ok: true, registry: { hosts: [{ id: "azure", transport: "http", url: "http://azure:7433" }] } })) as never);
      c.set("remoteFetchImpl" as never, (async (url: string, init: RequestInit) => {
        remoteCalls++;
        setSelfHostId("azure");
        try {
          const result = await remoteApp.request(new URL(url).pathname, init);
          if (mode === "stale-source" && remoteCalls === 1) {
            const original = db.prepare("SELECT qitem_id FROM queue_items LIMIT 1").get() as { qitem_id: string };
            repo.update({ qitemId: original.qitem_id, actorSession: "worker@team@azure", summary: "Changed authority while remote request awaited" });
          }
          if ((mode === "stage-loss" && remoteCalls === 1) || (mode === "activation-loss" && remoteCalls === 2)) throw new Error("Connection lost after target commit");
          return result;
        } finally { setSelfHostId("mac"); }
      }) as never);
      await next();
    });
    localApp.route("/api/mission-control", humanResponsesRoutes({ queueRepo: repo, bearerToken: "local-operator" }));
    setSelfHostId("mac");
    try {
      const item = await repo.create({ sourceSession: "worker@team@azure", destinationSession: "human@kernel", body: "Need release authority", summary: "Release decision", evidenceRef: "https://example.com/evidence", humanIntent: "decision", humanQuestions: [question], nudge: false });
      const post = (kind: "clarification" | "custom", text: string, revision: string) => localApp.request("/api/mission-control/human-response", {
        method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer local-operator" },
        body: JSON.stringify({ qitemId: item.qitemId, actorSession: "human@kernel", requestRevision: revision, replyId: kind, response: { kind, text } }),
      });
      const initialRevision = item.humanRequestRevision!;
      let clarify = await post("clarification", "Which users?", initialRevision);
      if (mode === "stale-source") {
        expect(clarify.status).toBe(409);
        const staged = remoteDb.prepare("SELECT qitem_id FROM queue_items WHERE qitem_id LIKE 'human-reply-%'").get() as { qitem_id: string };
        expect(remoteRepo.getById(staged.qitem_id)).toMatchObject({ state: "blocked" });
        expect(remoteRepo.getById(staged.qitem_id)?.body).toContain("PROVISIONAL HUMAN RESPONSE");
        expect(() => remoteRepo.claim({ qitemId: staged.qitem_id, destinationSession: "worker@team" })).toThrow(/provisional/);
        expect(remoteDb.prepare("SELECT count(*) AS n FROM outbox_entries").get()).toEqual({ n: 0 });
        expect(repo.getById(item.qitemId)).toMatchObject({ state: "pending", humanAnswers: null });
        return;
      }
      if (mode === "stage-loss" || mode === "activation-loss") {
        expect(clarify.status).toBe(502);
        expect(repo.getById(item.qitemId)).toMatchObject({ state: "pending", humanAnswers: null });
        clarify = await post("clarification", "Which users?", initialRevision);
      }
      expect(clarify.status).toBe(200);
      const firstReceipt = await clarify.json();
      expect(firstReceipt).toMatchObject({ complete: false, replyHostId: "azure" });
      expect(repo.getById(firstReceipt.replyQitemId)).toBeNull();
      expect(remoteRepo.getById(firstReceipt.replyQitemId)).toMatchObject({ sourceSession: "human@kernel@mac", destinationSession: "worker@team", state: "pending" });
      const callsBeforeRetry = remoteCalls;
      expect(await (await post("clarification", "Which users?", initialRevision)).json()).toEqual(firstReceipt);
      expect(remoteCalls).toBe(callsBeforeRetry + 1);
      const answer = await post("custom", "Test users only.", repo.getById(item.qitemId)!.humanRequestRevision!);
      expect(answer.status).toBe(200);
      const finalReceipt = await answer.json();
      expect(finalReceipt).toMatchObject({ complete: true, replyHostId: "azure" });
      expect(repo.getById(item.qitemId)).toMatchObject({ state: "done", humanAnswers: { release: { text: "Test users only." } } });
      expect(remoteRepo.getById(finalReceipt.replyQitemId)).toMatchObject({ destinationSession: "worker@team", state: "pending", claimedAt: null });
      expect(remoteRepo.getById(replacement.qitemId)?.state).toBe("in-progress");
      expect(remoteDb.prepare("SELECT count(*) AS n FROM queue_items WHERE state='in-progress'").get()).toEqual({ n: 1 });
      expect(remoteDb.prepare("SELECT count(*) AS n FROM outbox_entries").get()).toEqual({ n: 2 });
      expect(db.prepare("SELECT count(*) AS n FROM queue_items").get()).toEqual({ n: 1 });
    } finally { setSelfHostId(null); remoteDb.close(); }
  });

  it("recovers committed source activation from native outbox after reconstruction without browser state", async () => {
    const targetDb = createDb();
    migrate(targetDb, [coreSchema, eventsSchema, queueItemsSchema, queueTransitionsSchema, outboxEntriesSchema,
      queueItemSummarySchema, queueItemEvidenceRefSchema, humanNotificationIntentSchema, humanReplyToSchema, humanQuestionsSchema]);
    const targetRepo = new QueueRepository(targetDb, new EventBus(targetDb), { validateRig: session => session === "worker@team" });
    targetRepo.attachOutbox(new OutboxHandler(targetDb));
    const targetApp = new Hono().route("/api/mission-control", humanResponsesRoutes({ queueRepo: targetRepo }));
    setSelfHostId("mac");
    try {
      const item = await repo.create({ sourceSession: "worker@team@azure", destinationSession: "human@kernel", body: "Release authority", summary: "Release decision", evidenceRef: "https://example.com/evidence", humanIntent: "decision", humanQuestions: [question], nudge: false });
      const input = { qitemId: item.qitemId, requestRevision: item.humanRequestRevision!, replyId: "committed-before-crash", actorSession: "human@kernel", response: { kind: "custom" as const, text: "Test users only." } };
      const plan = repo.prepareHumanResponse(input);
      if (plan.status !== "prepared") throw new Error("Expected an unrecorded response");
      const staged = targetRepo.stageHumanResponseReturn({ ...plan.reply, sourceSession: "human@kernel@mac" });
      repo.recordHumanResponse(input, staged);
      expect(repo.getById(item.qitemId)?.state).toBe("done");
      expect(targetRepo.getById(staged.qitemId)?.state).toBe("blocked");
      expect(targetDb.prepare("SELECT count(*) AS n FROM outbox_entries").get()).toEqual({ n: 0 });
      // Rebuild source services as after a daemon restart. No browser reply ID or answer text is supplied.
      const restarted = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
      restarted.attachOutbox(new OutboxHandler(db));
      let forwards = 0;
      const deps = {
        loadRegistry: () => ({ ok: true as const, registry: { hosts: [{ id: "azure", transport: "http" as const, url: "http://azure:7433" }] } }),
        fetchImpl: (async (url: string, init: RequestInit) => {
          forwards++; setSelfHostId("azure");
          try { return await targetApp.request(new URL(url).pathname, init); }
          finally { setSelfHostId("mac"); }
        }) as typeof fetch,
      };
      expect(await recoverHumanResponseReturns(restarted, deps)).toEqual({ activated: 1, pending: 0, errors: [] });
      expect(targetRepo.getById(staged.qitemId)).toMatchObject({ state: "pending", body: plan.reply.body });
      expect(targetDb.prepare("SELECT count(*) AS n FROM outbox_entries").get()).toEqual({ n: 1 });
      expect(await recoverHumanResponseReturns(restarted, deps)).toEqual({ activated: 0, pending: 0, errors: [] });
      expect(forwards).toBe(1);
      // Four unavailable oldest targets back off; a fifth intent must be due on the next pass.
      const outbox = new OutboxHandler(db);
      for (let i = 0; i < 5; i++) outbox.record({ outboxId: `${HUMAN_RESPONSE_ACTIVATION_PREFIX}fair-${i}`,
        senderSession: "human@kernel", destinationSession: "worker@team@azure",
        body: JSON.stringify({ replyQitemId: `fair-${i}`, replyHostId: "azure", actorSession: "human@kernel@mac", fingerprint: "f".repeat(64) }) });
      let failures = 0;
      const failed = await recoverHumanResponseReturns(restarted, { ...deps, fetchImpl: (async () => {
        failures++; throw new Error("Temporarily unavailable");
      }) as typeof fetch });
      expect(failed).toMatchObject({ activated: 0, pending: 4 });
      expect(failures).toBe(4);
      expect(restarted.pendingHumanResponseActivations().map(intent => intent.outboxId)).toEqual([`${HUMAN_RESPONSE_ACTIVATION_PREFIX}fair-4`]);
    } finally { setSelfHostId(null); targetDb.close(); }
  });

});
