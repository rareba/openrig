import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Hono } from "hono";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { eventsSchema } from "../src/db/migrations/003_events.js";
import { streamItemsSchema } from "../src/db/migrations/023_stream_items.js";
import { queueItemsSchema } from "../src/db/migrations/024_queue_items.js";
import { queueTransitionsSchema } from "../src/db/migrations/025_queue_transitions.js";
import { inboxEntriesSchema } from "../src/db/migrations/026_inbox_entries.js";
import { outboxEntriesSchema } from "../src/db/migrations/027_outbox_entries.js";
import { queueTargetRepoSchema } from "../src/db/migrations/039_queue_target_repo.js";
import { i3IdentityProvenanceSchema } from "../src/db/migrations/067_i3_identity_provenance.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { InboxHandler } from "../src/domain/inbox-handler.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { queueRoutes } from "../src/routes/queue.js";

// ── P18 SWEEP — the provenance-laundering RED (dev50-driver's sealed finding).
//
// Deleting the requireSenderIdentity refusal MANUFACTURES a header-absent delivery path that
// could not previously exist: a caller with no X-OpenRig-Session but a body-declared actor is now
// delivered under that actor labelled `claimed:v1`. The route sites then hardcode
// `identityProvenance: "transport:v1"` — stamping the row as wire-CERTIFIED when it was only
// body-CLAIMED. That is unverified laundered into verified, which resolveRecordedProvenance's own
// contract forbids. tsc is 0 with the defect present and every header-present test passes; only THIS
// path exposes it. The fix threads resolveRecordedProvenance(c, identity) at each site.
function buildApp(opts: {
  eventBus: EventBus;
  queueRepo: QueueRepository;
  inboxHandler: InboxHandler;
  outboxHandler: OutboxHandler;
}): Hono {
  const app = new Hono();
  app.use("*", async (c, next) => {
    c.set("eventBus" as never, opts.eventBus);
    c.set("queueRepo" as never, opts.queueRepo);
    c.set("inboxHandler" as never, opts.inboxHandler);
    c.set("outboxHandler" as never, opts.outboxHandler);
    await next();
  });
  app.route("/api/queue", queueRoutes());
  return app;
}

describe("P18 provenance thread — header-absent delivery must not launder claimed:v1 → transport:v1", () => {
  let db: Database.Database;
  let bus: EventBus;
  let queueRepo: QueueRepository;
  let inbox: InboxHandler;
  let outbox: OutboxHandler;
  let app: Hono;

  beforeEach(() => {
    db = createDb();
    migrate(db, [
      coreSchema,
      eventsSchema,
      streamItemsSchema,
      queueItemsSchema,
      queueTransitionsSchema,
      inboxEntriesSchema,
      outboxEntriesSchema,
      queueTargetRepoSchema,
      i3IdentityProvenanceSchema,
    ]);
    bus = new EventBus(db);
    queueRepo = new QueueRepository(db, bus);
    inbox = new InboxHandler(db, bus, queueRepo);
    outbox = new OutboxHandler(db);
    queueRepo.attachOutbox(outbox);
    app = buildApp({ eventBus: bus, queueRepo, inboxHandler: inbox, outboxHandler: outbox });
  });

  afterEach(() => db.close());

  // outbox_entries table — outbox record (queue.ts:1021)
  it("outbox record: header ABSENT + body senderSession → delivers, records claimed:v1 (NOT transport:v1)", async () => {
    const res = await app.request("/api/queue/outbox/record", {
      method: "POST",
      headers: { "Content-Type": "application/json" }, // NO X-OpenRig-Session — the manufactured path
      body: JSON.stringify({ senderSession: "claimant@rig", destinationSession: "dest@rig", body: "hi" }),
    });
    expect(res.status).toBe(201); // deliver-and-label, not refuse
    const { outboxId } = (await res.json()) as { outboxId: string };
    const row = db
      .prepare("SELECT identity_provenance FROM outbox_entries WHERE outbox_id = ?")
      .get(outboxId) as { identity_provenance: string | null } | undefined;
    expect(row?.identity_provenance).toBe("claimed:v1");
  });

  // POSITIVE CONTROL — the header-present path is byte-unchanged (still transport:v1).
  it("outbox record: header PRESENT → records transport:v1 (certified path unchanged)", async () => {
    const res = await app.request("/api/queue/outbox/record", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-OpenRig-Session": "me@rig" },
      body: JSON.stringify({ destinationSession: "dest@rig", body: "hi" }),
    });
    expect(res.status).toBe(201);
    const { outboxId } = (await res.json()) as { outboxId: string };
    const row = db
      .prepare("SELECT identity_provenance FROM outbox_entries WHERE outbox_id = ?")
      .get(outboxId) as { identity_provenance: string | null } | undefined;
    expect(row?.identity_provenance).toBe("transport:v1");
  });

  it.each([{ "x-openrig-origin-unknown": "true" }, { "x-openrig-relay": "true", "x-openrig-provenance": "origin-unknown:v1" }])("queue create preserves unknown origin durably across direct and relayed delivery: %j", async (relayHeaders) => {
    const res = await app.request("/api/queue/create", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-OpenRig-Session": "me@rig", ...relayHeaders },
      body: JSON.stringify({ destinationSession: "dest@rig", body: "origin unavailable" }),
    });
    expect(res.status).toBe(201);
    const { qitemId } = await res.json() as { qitemId: string };
    expect(db.prepare("SELECT source_session FROM queue_items WHERE qitem_id = ?").get(qitemId)).toEqual({ source_session: "me@rig" });
    expect(db.prepare("SELECT identity_provenance FROM queue_transitions WHERE qitem_id = ? ORDER BY rowid LIMIT 1").get(qitemId)).toEqual({ identity_provenance: "origin-unknown:v1" });
  });

  it.each([['transport:v1','relay:v1'],['relay:v1','relay:v1'],['claimed:v1','claimed:v1'],['invalid','claimed:v1'],['origin-unknown:v1','origin-unknown:v1']])('preserves or degrades carried provenance %s to %s across another native hop',async(carried,expected)=>{
    const response=await app.request('/api/queue/create',{method:'POST',headers:{'Content-Type':'application/json','X-OpenRig-Session':'me@rig','X-OpenRig-Relay':'coordinator','X-OpenRig-Provenance':carried!},body:JSON.stringify({destinationSession:'dest@rig',body:'relayed'})});
    expect(response.status).toBe(201);const item=await response.json() as {qitemId:string};expect(db.prepare('SELECT identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY rowid LIMIT 1').get(item.qitemId)).toEqual({identity_provenance:expected});
  });

  // queue_transitions table — create (queue.ts:470)
  it("queue create: header ABSENT + body sourceSession → delivers, transition records claimed:v1 (NOT transport:v1)", async () => {
    const res = await app.request("/api/queue/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" }, // NO X-OpenRig-Session
      body: JSON.stringify({ destinationSession: "dest@rig", body: "hi", sourceSession: "claimant@rig" }),
    });
    expect(res.status).toBe(201);
    const { qitemId } = (await res.json()) as { qitemId: string };
    const row = db
      .prepare("SELECT identity_provenance FROM queue_transitions WHERE qitem_id = ? ORDER BY rowid ASC LIMIT 1")
      .get(qitemId) as { identity_provenance: string | null } | undefined;
    expect(row?.identity_provenance).toBe("claimed:v1");
  });

  it("queue create: header PRESENT → transition records transport:v1 (certified path unchanged)", async () => {
    const res = await app.request("/api/queue/create", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-OpenRig-Session": "me@rig" },
      body: JSON.stringify({ destinationSession: "dest@rig", body: "hi" }),
    });
    expect(res.status).toBe(201);
    const { qitemId } = (await res.json()) as { qitemId: string };
    const row = db
      .prepare("SELECT identity_provenance FROM queue_transitions WHERE qitem_id = ? ORDER BY rowid ASC LIMIT 1")
      .get(qitemId) as { identity_provenance: string | null } | undefined;
    expect(row?.identity_provenance).toBe("transport:v1");
  });
});
