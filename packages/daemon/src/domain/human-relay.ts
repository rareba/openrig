import { createHash, randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { OutboxHandler } from "./outbox-handler.js";
import { parseHumanResponse, unansweredQuestions, type HumanQuestion } from "./human-questions.js";
import { QueueRepositoryError, type HumanResponseInput, type QueueRepository } from "./queue-repository.js";
/** Dedicated ownership: these records never enter the executable wake-intent namespace. */
export const HUMAN_RELAY_PREFIX = "human-relay-";
export interface HumanRelayBinding {
    connectorId: string;
    humanSession: string;
    profile: string;
    chatId: string;
    senderId: string;
}
export interface HumanRelayDelivery extends HumanRelayBinding {
    deliveryId: string;
    attemptId: string;
    originHostId: string;
    qitemId: string;
    requestRevision: string;
    summary: string | null;
    body: string;
    evidenceRef: string | null;
    questions: HumanQuestion[];
}
interface Envelope {
    delivery: HumanRelayDelivery;
    leaseUntil: number;
    retryAt: number;
    attempts: number;
    messageIds: string[];
}
const digest = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
function fail(message: string): never { throw new QueueRepositoryError("invalid_human_relay", message); }
function object(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : fail("A relay object is required."); }
function text(value: unknown): string { return typeof value === "string" && value.length > 0 && value.length <= 1000 ? value : fail("A nonempty bounded relay identity is required."); }
export function loadHumanRelayBindings(home: string): HumanRelayBinding[] {
    const file = join(home, "human-relay.json");
    if (!existsSync(file))
        return [];
    const config = object(JSON.parse(readFileSync(file, "utf8")));
    if (config.version !== 1 || !Array.isArray(config.bindings) || config.bindings.length > 10)
        fail("human-relay.json requires version 1 and at most ten explicit bindings.");
    return config.bindings.map(raw => {
        const b = object(raw);
        const result = { connectorId: text(b.connectorId), humanSession: text(b.humanSession), profile: text(b.profile), chatId: text(b.chatId), senderId: text(b.senderId) };
        if (result.humanSession !== "human@kernel")
            fail("This relay supports explicitly bound human@kernel only.");
        return result;
    });
}
/** The queue owns decisions. The existing native outbox owns delivery audit/claims, never work. */
export class HumanRelayService {
    private outbox: OutboxHandler;
    private cursors = new Map<string, {
        ts: string;
        id: string;
    }>();
    constructor(private db: Database.Database, private queue: QueueRepository, private deps: {
        bindings: () => HumanRelayBinding[];
        hostId: () => string | null;
        now?: () => number;
    }) { this.outbox = new OutboxHandler(db); this.outbox.reconcileAbandonedSending(HUMAN_RELAY_PREFIX); }
    private now() { return this.deps.now?.() ?? Date.now(); }
    private bindings() { const list = this.deps.bindings(); for (const b of list) {
        if (b.humanSession !== "human@kernel")
            fail("An explicit canonical local human binding is required.");
        for (const v of Object.values(b))
            text(v);
    } return list; }
    private read(id: string): {
        envelope: Envelope;
        state: string;
    } {
        if (!id.startsWith(HUMAN_RELAY_PREFIX))
            fail("Unknown relay delivery.");
        const row = this.outbox.getById(id);
        if (!row)
            throw new QueueRepositoryError("human_relay_not_found", "Unknown relay delivery.");
        return { envelope: JSON.parse(row.body) as Envelope, state: row.deliveryState };
    }
    private write(id: string, e: Envelope, state: string) { this.db.prepare("UPDATE outbox_entries SET body=?,delivery_state=?,delivered_at=? WHERE outbox_id=?").run(JSON.stringify(e), state, state === "delivered" ? new Date(this.now()).toISOString() : null, id); }
    private bindingFor(d: HumanRelayDelivery) { const current = this.bindings().find(b => Object.entries(b).every(([k, v]) => d[k as keyof HumanRelayBinding] === v)); if (!current)
        fail("The human relay binding changed; refresh operator configuration."); return current; }
    pull(raw: unknown): {
        deliveries: HumanRelayDelivery[];
        indeterminate: number;
    } {
        const input = object(raw);
        const connectorId = text(input.connectorId), profile = text(input.profile), now = this.now();
        const maxDeliveries = Number.isInteger(input.limit) && Number(input.limit) >= 1 && Number(input.limit) <= 4 ? Number(input.limit) : 4;
        const host = this.deps.hostId();
        if (!host)
            fail("The origin host must have a stable native identity.");
        return this.db.transaction(() => {
            const active = this.db.prepare("SELECT outbox_id,body FROM outbox_entries WHERE substr(outbox_id,1,?)=? AND delivery_state='sending' LIMIT 200").all(HUMAN_RELAY_PREFIX.length, HUMAN_RELAY_PREFIX) as Array<{
                outbox_id: string;
                body: string;
            }>;
            for (const r of active) {
                const e = JSON.parse(r.body) as Envelope;
                if (e.leaseUntil <= now)
                    this.write(r.outbox_id, e, "indeterminate");
            }
            const deliveries: HumanRelayDelivery[] = [];
            for (const b of this.bindings().filter(b => b.connectorId === connectorId && b.profile === profile)) {
                const key = digest(b);
                const cursor = this.cursors.get(key) ?? { ts: "", id: "" };
                // One bounded page per binding. Cursor wraps after a short page, so old delivered/held
                // questions never permanently hide later questions, including across repeated polls.
                const candidates = this.db.prepare(`SELECT qitem_id,ts_created FROM queue_items WHERE destination_session=? AND state='pending'
     AND (ts_created>? OR (ts_created=? AND qitem_id>?)) ORDER BY ts_created,qitem_id LIMIT 50`).all(b.humanSession, cursor.ts, cursor.ts, cursor.id) as Array<{
                    qitem_id: string;
                    ts_created: string;
                }>;
                this.cursors.set(key, candidates.length === 50 ? { ts: candidates.at(-1)!.ts_created, id: candidates.at(-1)!.qitem_id } : { ts: "", id: "" });
                for (const c of candidates) {
                    if (deliveries.length >= maxDeliveries)
                        break;
                    const q = this.queue.getById(c.qitem_id);
                    if (!q || q.state !== "pending" || q.humanIntent === "update" || !q.humanRequestRevision)
                        continue;
                    const id = HUMAN_RELAY_PREFIX + digest([host, q.qitemId, q.humanRequestRevision, b, "whatsapp"]);
                    const prior = this.outbox.getById(id);
                    let e: Envelope;
                    if (prior) {
                        if (prior.deliveryState !== "pending")
                            continue;
                        e = JSON.parse(prior.body) as Envelope;
                        if (e.retryAt > now)
                            continue;
                    }
                    else {
                        e = { delivery: { ...b, deliveryId: id, attemptId: "", originHostId: host, qitemId: q.qitemId, requestRevision: q.humanRequestRevision, summary: q.summary, body: q.body, evidenceRef: q.evidenceRef, questions: unansweredQuestions(q.humanQuestions ?? [], q.humanAnswers ?? {}) }, leaseUntil: 0, retryAt: 0, attempts: 0, messageIds: [] };
                        this.outbox.record({ outboxId: id, senderSession: q.sourceSession, destinationSession: b.humanSession, body: JSON.stringify(e), tags: ["human-relay", "channel:whatsapp"], auditPointer: q.qitemId });
                    }
                    // Current state/revision/recipient and claim are inside the same SQLite transaction.
                    e.delivery.attemptId = randomUUID();
                    e.attempts++;
                    e.leaseUntil = now + 120000;
                    this.write(id, e, "sending");
                    deliveries.push(e.delivery);
                }
            }
            const count = this.db.prepare("SELECT count(*) AS n FROM outbox_entries WHERE substr(outbox_id,1,?)=? AND delivery_state='indeterminate'").get(HUMAN_RELAY_PREFIX.length, HUMAN_RELAY_PREFIX) as {
                n: number;
            };
            return { deliveries, indeterminate: count.n };
        })();
    }
    receipt(raw: unknown) {
        const r = object(raw);
        return this.db.transaction(() => {
            const id = text(r.deliveryId), { envelope: e, state } = this.read(id);
            this.bindingFor(e.delivery);
            if (r.attemptId !== e.delivery.attemptId)
                fail("The platform receipt names a different attempt.");
            if (r.outcome === "platform-sent") {
                if (!Array.isArray(r.messageIds) || r.messageIds.length < 1 || r.messageIds.length > 20 || r.messageIds.some(v => typeof v !== "string" || !v || v.length > 1000))
                    fail("Actual platform message receipts are required.");
                if (state === "delivered") {
                    if (JSON.stringify(e.messageIds) !== JSON.stringify(r.messageIds))
                        fail("A different platform receipt is already recorded.");
                    return { state };
                }
                if (!["sending", "indeterminate"].includes(state))
                    fail("This delivery cannot accept a platform receipt.");
                e.messageIds = r.messageIds.map(value => text(value));
                this.write(id, e, "delivered");
                return { state: "delivered" };
            }
            if (r.outcome === "not-attempted") {
                // The same trusted attempt can prove no send occurred even after a
                // daemon restart conservatively marked its claim indeterminate.
                if (state !== "sending" && state !== "indeterminate")
                    return { state };
                e.retryAt = this.now() + Math.min(60000 * 2 ** Math.min(e.attempts - 1, 5), 1800000);
                this.write(id, e, "pending");
                return { state: "pending" };
            }
            if (state !== "sending")
                return { state };
            if (r.outcome !== "unknown")
                fail("A platform receipt, definite not-attempted outcome, or unknown outcome is required.");
            this.write(id, e, "indeterminate");
            return { state: "indeterminate" };
        })();
    }
    private correlate(raw: unknown) {
        const r = object(raw), { envelope: e, state } = this.read(text(r.deliveryId));
        const b = this.bindingFor(e.delivery);
        for (const k of ["connectorId", "profile", "chatId", "senderId"] as const)
            if (r[k] !== b[k])
                fail("The native message does not match the bound human/profile/chat/sender.");
        if (state !== "delivered" || !e.messageIds.includes(text(r.replyToMessageId)))
            fail("Reply to the known platform question message.");
        return { r, e };
    }
    context(raw: unknown) { const { e } = this.correlate(raw), q = this.queue.getById(e.delivery.qitemId); return { delivery: e.delivery, current: q?.state === "pending" && q.humanRequestRevision === e.delivery.requestRevision, requestState: q?.state ?? "missing", questions: q ? unansweredQuestions(q.humanQuestions ?? [], q.humanAnswers ?? {}) : e.delivery.questions, summary: q?.summary ?? e.delivery.summary, body: q?.body ?? e.delivery.body, evidenceRef: q?.evidenceRef ?? e.delivery.evidenceRef }; }
    responseInput(raw: unknown): HumanResponseInput {
        const { r, e } = this.correlate(raw), d = e.delivery;
        const response = parseHumanResponse(r.response);
        if (!response)
            fail("Provide an explicit native choice, custom answer, or clarification.");
        if (r.qitemId !== d.qitemId || r.requestRevision !== d.requestRevision)
            fail("The original question identity/revision must be preserved.");
        return { qitemId: d.qitemId, requestRevision: d.requestRevision, actorSession: d.humanSession, replyId: "hermes-" + digest([d.connectorId, d.profile, d.chatId, d.senderId, text(r.messageId)]), response };
    }
}
