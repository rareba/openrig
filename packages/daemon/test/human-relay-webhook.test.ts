import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { humanRelayRoutes } from "../src/routes/human-relay.js";
import type Database from "better-sqlite3";
import { createHash, createHmac } from "node:crypto";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { HumanRelayService, type HumanRelayBinding } from "../src/domain/human-relay.js";
import { HumanRelayWebhookDispatcher, type HumanRelayWebhookTarget } from "../src/domain/human-relay-webhook.js";
const binding: HumanRelayBinding = { connectorId: "hermes", humanSession: "human@kernel", profile: "default", chatId: "chat", senderId: "person" };
const target: HumanRelayWebhookTarget = { connectorId: "hermes", profile: "default", channel: "whatsapp", endpointUrl: "http://127.0.0.1:8644/webhooks/openrig-human-relay", secretFile: "/configured/secret" };
describe("durable human webhook delivery", () => {
  let db: Database.Database, queue: QueueRepository, service: HumanRelayService, now: number;
  beforeEach(() => { db = createDb(); migrate(db, ALL_MIGRATIONS); queue = new QueueRepository(db, new EventBus(db), { validateRig: () => true }); now = 100000; service = make(); });
  afterEach(() => db.close());
  function make(targets = [target], bindings = [binding]) { return new HumanRelayService(db, queue, { bindings: () => bindings, webhooks: () => targets, hostId: () => "origin", now: () => now }); }
  const ask = () => queue.create({ sourceSession: "manager@team", destinationSession: "human@kernel", body: "Synthetic", summary: "Sample", evidenceRef: "/synthetic", humanIntent: "decision", humanQuestions: [{ id: "q", question: "Which?", options: [{ id: "a", label: "A" }, { id: "b", label: "B" }] }], nudge: false });
  it("retains the same claim after lost acceptance and restart, verifies signed durable acceptance separately from platform receipt", async () => {
    const q = await ask();
    expect(service.pull({ connectorId: "hermes", profile: "default" }).deliveries).toEqual([]);
    const calls: Array<{ body: string; headers: Record<string,string> }> = [];
    let fail = true;
    const fetchImpl = async (_url: unknown, init?: RequestInit) => {
      const body = String(init?.body), headers = init?.headers as Record<string,string>;
      calls.push({ body, headers });
      const expected = createHmac("sha256", "secret").update(`${headers["webhook-id"]}.${headers["webhook-timestamp"]}.${body}`).digest("base64");
      expect(headers["webhook-signature"]).toBe(`v1,${expected}`);
      if (fail) throw Error("lost acceptance");
      const event = JSON.parse(body);
      return new Response(JSON.stringify({ schema: event.schema, status: "accepted", deliveryId: event.deliveryId, fingerprint: event.fingerprint }));
    };
    const dispatcher = () => new HumanRelayWebhookDispatcher(service, { targets: () => [target], readSecret: () => "secret", fetchImpl: fetchImpl as typeof fetch, now: () => now });
    expect((await dispatcher().reconcile()).error).toBeTruthy();
    const first = JSON.parse(calls[0]!.body).delivery;
    expect(first.qitemId).toBe(q.qitemId);
    const legacyId = "human-relay-" + createHash("sha256").update(JSON.stringify(["origin", q.qitemId, q.humanRequestRevision, binding, "whatsapp"])).digest("hex");
    expect(first.deliveryId).toBe(legacyId);
    expect(new OutboxHandler(db).getById(first.deliveryId)?.deliveryState).not.toBe("delivered");
    service = make(); now += 61000; fail = false;
    expect((await dispatcher().reconcile()).error).toBeNull();
    expect(calls[1]!.body).toBe(calls[0]!.body);
    expect(service.validate(first).current).toBe(true);
    expect(new OutboxHandler(db).getById(first.deliveryId)?.deliveryState).not.toBe("delivered");
    now += 61000; await dispatcher().reconcile(); expect(calls).toHaveLength(2);
    service.receipt({ ...first, outcome: "platform-sent", messageIds: ["real-platform-id"] });
    expect(new OutboxHandler(db).getById(first.deliveryId)?.deliveryState).toBe("delivered");
  });
  it("rejects bare HTTP success, revalidates pending requests, and holds changed targets and identity", async () => {
    const q = await ask(); let sends = 0; const sentIds: string[] = [];
    const dispatcher = new HumanRelayWebhookDispatcher(service, { targets: () => [target], readSecret: () => "secret", fetchImpl: (async (_url: unknown, init?: RequestInit) => { sends++; sentIds.push(JSON.parse(String(init?.body)).deliveryId); return new Response('{}'); }) as typeof fetch, now: () => now });
    expect((await dispatcher.reconcile()).error).toBeTruthy();
    now += 61000;
    const d = service.webhookBatch(target)[0]!.delivery;
    expect(() => service.validate({ ...d, senderId: "other" })).toThrow();
    expect(() => service.validate({ ...d, attemptId: "other" })).toThrow();
    const changed = { ...target, endpointUrl: "http://127.0.0.1:8644/hooks/other" };
    expect(make([changed]).webhookBatch(changed)).toEqual([]);
    expect(() => make([changed]).validate(d)).toThrow(/transport changed/);
    expect(() => make([]).validate(d)).toThrow(/transport changed/);
    queue.update({ qitemId: q.qitemId, actorSession: "manager@team", summary: "Revised" });
    expect(service.validate(d).current).toBe(false);
    now += 61000; await dispatcher.reconcile(); expect(sends).toBe(2); expect(sentIds.filter(id => id === d.deliveryId)).toHaveLength(1);
  });
  it("separates non-WhatsApp identities and validates before platform receipt without fabricating one", async () => {
    await ask();
    const slack = { ...binding, channel: "slack" as const };
    const s = make([], [binding, slack]);
    const claims = s.pull({ connectorId: "hermes", profile: "default" }).deliveries;
    expect(claims).toHaveLength(2); expect(claims[0]!.deliveryId).not.toBe(claims[1]!.deliveryId);
    for (const d of claims) { expect(s.validate(d).current).toBe(true); expect(() => s.context({ ...d, replyToMessageId: "invented" })).toThrow(); }
  });
  it("isolates outbound-only bridges from push and requires an exact authenticated acceptance", async () => {
    await ask(); const bridge = {...target, mode: "pull-bridge" as const}; service = make([bridge]);
    let sends = 0;
    const dispatcher = new HumanRelayWebhookDispatcher(service, {targets: () => [bridge], fetchImpl: (async () => {sends++; throw Error();}) as typeof fetch});
    expect(await dispatcher.reconcile()).toEqual({error: null}); expect(sends).toBe(0);
    const args = {connectorId: "hermes", profile: "default", channel: "whatsapp"};
    const event = service.webhookPull(args).events[0]!;
    service = make([bridge]);
    expect(service.webhookPull(args).events[0]).toEqual(event);
    expect(() => service.webhookAccepted({...event.delivery, fingerprint: "other"})).toThrow();
    expect(() => service.webhookAccepted({...event.delivery, fingerprint: event.fingerprint, senderId: "other"})).toThrow();
    expect(service.webhookAccepted({...event.delivery, fingerprint: event.fingerprint})).toEqual({state: "accepted"});
    expect(service.webhookPull(args).events).toEqual([]);
    expect(new OutboxHandler(db).getById(event.deliveryId)?.deliveryState).not.toBe("delivered");
    expect(() => make([target]).validate(event.delivery)).toThrow(/transport changed/);
  });

  it("advances past a full batch of failed retries even when every poll follows the retry cap", async () => {
    const questions = await Promise.all([ask(), ask(), ask(), ask()]);
    const seen = new Set<string>();
    const dispatcher = new HumanRelayWebhookDispatcher(service, {targets: () => [target], readSecret: () => "secret", now: () => now, fetchImpl: (async (_url: unknown, init?: RequestInit) => { seen.add(JSON.parse(String(init?.body)).delivery.qitemId); throw Error("offline"); }) as typeof fetch});
    for (let i=0; i<8; i++) { await dispatcher.reconcile(); now += 301000; }
    expect([...seen].sort()).toEqual(questions.map(q => q.qitemId).sort());
  });

  it("accepts the real bridge callback shape through authenticated native routes", async () => {
    await ask(); const bridge = {...target, mode: "pull-bridge" as const}; service = make([bridge]);
    const app = humanRelayRoutes({service, queueRepo: queue, bearerToken: "native-secret"});
    const post = (operation: string, value: unknown, auth = true) => app.request(`/human-relay/${operation}`, {method: "POST", headers: {"Content-Type": "application/json", ...(auth ? {Authorization: "Bearer native-secret"} : {})}, body: JSON.stringify(value)});
    expect((await post("webhook-pull", binding, false)).status).toBe(401);
    const pulled = await post("webhook-pull", {connectorId: "hermes", profile: "default", channel: "whatsapp"});
    expect(pulled.status).toBe(200); const {events} = await pulled.json(); const e = events[0]; const d = e.delivery;
    const payload = {connectorId: d.connectorId, profile: d.profile, channel: d.channel ?? "whatsapp", chatId: d.chatId, senderId: d.senderId, deliveryId: d.deliveryId, attemptId: d.attemptId, fingerprint: e.fingerprint};
    expect((await post("webhook-accepted", {...payload, senderId: undefined})).status).toBe(409);
    const accepted = await post("webhook-accepted", payload); expect(accepted.status).toBe(200); expect(await accepted.json()).toEqual({state: "accepted"});
    const valid = await post("validate", d); expect(valid.status).toBe(200); expect((await valid.json()).current).toBe(true);
    expect(new OutboxHandler(db).getById(d.deliveryId)?.deliveryState).not.toBe("delivered");
  });

});
