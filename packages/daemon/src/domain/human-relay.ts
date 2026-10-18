import { createHash, randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { relayFingerprint, type HumanRelayChannel, type HumanRelayWebhookTarget, type HumanRelayWebhookEnvelope } from "./human-relay-webhook.js";
import { OutboxHandler } from "./outbox-handler.js";
import { parseHumanResponse, unansweredQuestions, type HumanQuestion } from "./human-questions.js";
import { QueueRepositoryError, type HumanResponseInput, type QueueRepository } from "./queue-repository.js";
/** Dedicated ownership: these records never enter the executable wake-intent namespace. */
export const HUMAN_RELAY_PREFIX = "human-relay-";
export interface HumanRelayBinding {
    channel?: HumanRelayChannel;
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
    webhook?: { fingerprint: string; endpoint: string; accepted: boolean; retryAt: number; failures: number; mode: "push" | "pull-bridge" };
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
        if (b.channel !== undefined) {
            if (!["whatsapp", "slack", "teams"].includes(String(b.channel))) fail("Unsupported human relay channel.");
            if (b.channel !== "whatsapp") (result as HumanRelayBinding).channel = b.channel as HumanRelayChannel;
        }
        return result;
    });
}
/** The queue owns decisions. The existing native outbox owns delivery audit/claims, never work. */
export class HumanRelayService {
    private outbox: OutboxHandler;
    private webhookCursors = new Map<string, number>();
    private cursors = new Map<string, {
        ts: string;
        id: string;
    }>();
    constructor(private db: Database.Database, private queue: QueueRepository, private deps: {
        bindings: () => HumanRelayBinding[];
        hostId: () => string | null;
        webhooks?: () => HumanRelayWebhookTarget[];
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
    private bindingFor(d: HumanRelayDelivery) { const current = this.bindings().find(b => ["connectorId", "humanSession", "profile", "chatId", "senderId"].every(k => d[k as keyof HumanRelayBinding] === b[k as keyof HumanRelayBinding]) && (b.channel ?? "whatsapp") === (d.channel ?? "whatsapp")); if (!current)
        fail("The human relay binding changed; refresh operator configuration."); return current; }
    pull(raw: unknown): {
        deliveries: HumanRelayDelivery[];
        indeterminate: number;
    } {
        return this.claim(raw);
    }
    private claim(raw: unknown, target?: HumanRelayWebhookTarget): { deliveries: HumanRelayDelivery[]; indeterminate: number } {
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
                if (e.leaseUntil <= now && !e.webhook)
                    this.write(r.outbox_id, e, "indeterminate");
            }
            const deliveries: HumanRelayDelivery[] = [];
            for (const b of this.bindings().filter(b => b.connectorId === connectorId && b.profile === profile)) {
                const configured = this.deps.webhooks?.().find(t => t.connectorId === b.connectorId && t.profile === b.profile && t.channel === (b.channel ?? "whatsapp"));
                if (target ? configured?.endpointUrl !== target.endpointUrl || (b.channel ?? "whatsapp") !== target.channel : Boolean(configured)) continue;
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
                    if (!q || q.state !== "pending" || q.humanIntent === "update" || !q.humanRequestRevision || q.humanForwardingStatus)
                        continue;
                    const id = HUMAN_RELAY_PREFIX + digest([host, q.qitemId, q.humanRequestRevision, b.channel === "whatsapp" ? { connectorId: b.connectorId, humanSession: b.humanSession, profile: b.profile, chatId: b.chatId, senderId: b.senderId } : b, b.channel ?? "whatsapp"]);
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
                        this.outbox.record({ outboxId: id, senderSession: q.sourceSession, destinationSession: b.humanSession, body: JSON.stringify(e), tags: ["human-relay", "channel:" + (b.channel ?? "whatsapp")], auditPointer: q.qitemId });
                    }
                    // Current state/revision/recipient and claim are inside the same SQLite transaction.
                    e.delivery.attemptId = randomUUID();
                    e.attempts++;
                    e.leaseUntil = now + 120000;
                    if (target) e.webhook = { fingerprint: relayFingerprint(e.delivery), endpoint: target.endpointUrl, mode: target.mode ?? "push", accepted: false, retryAt: 0, failures: 0 };
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
    webhookBatch(target: HumanRelayWebhookTarget): HumanRelayWebhookEnvelope[] {
        this.claim({ connectorId: target.connectorId, profile: target.profile, limit: 2 }, target);
        const key = JSON.stringify([target.connectorId, target.profile, target.channel]);
        const cursor = this.webhookCursors.get(key) ?? 0;
        const rows = this.db.prepare(`SELECT rowid AS seq,outbox_id,body FROM outbox_entries WHERE substr(outbox_id,1,?)=? AND delivery_state IN ('sending','indeterminate')
          AND json_extract(body,'$.webhook.accepted')=0 AND json_extract(body,'$.delivery.connectorId')=? AND json_extract(body,'$.delivery.profile')=?
          AND coalesce(json_extract(body,'$.delivery.channel'),'whatsapp')=? AND rowid>? ORDER BY rowid LIMIT 50`).all(HUMAN_RELAY_PREFIX.length, HUMAN_RELAY_PREFIX, target.connectorId, target.profile, target.channel, cursor) as {seq: number; outbox_id: string; body: string}[];

        const result: HumanRelayWebhookEnvelope[] = [];
        for (const row of rows) {
            this.webhookCursors.set(key, row.seq);
            const e = JSON.parse(row.body) as Envelope, w = e.webhook, d = e.delivery;
            if (!w || w.accepted || w.retryAt > this.now() || w.endpoint !== target.endpointUrl || w.mode !== (target.mode ?? "push") || d.connectorId !== target.connectorId || d.profile !== target.profile || (d.channel ?? "whatsapp") !== target.channel) continue;
            try { if (!this.validate(d).current) continue; } catch { continue; }
            result.push({delivery: d, fingerprint: w.fingerprint});
            if (result.length === 2) return result;
        }
        if (rows.length < 50) this.webhookCursors.set(key, 0);
        return result;
    }
    webhookPull(raw: unknown) {
        const r = object(raw), channel = r.channel ?? "whatsapp";
        const target = this.deps.webhooks?.().find(t => t.mode === "pull-bridge" && t.connectorId === r.connectorId && t.profile === r.profile && t.channel === channel);
        if (!target) fail("No configured outbound-only bridge matches this connector/profile/channel.");
        return { events: this.webhookBatch(target).map(({delivery, fingerprint}) => ({schema: "openrig.human-relay.v1", type: "question.created", deliveryId: delivery.deliveryId, fingerprint, delivery})) };
    }
    webhookAccepted(raw: unknown) {
        const r = object(raw), id = text(r.deliveryId), {envelope: e} = this.read(id);
        this.validate(r);
        if (e.webhook?.mode !== "pull-bridge" || r.fingerprint !== e.webhook.fingerprint) fail("A matching outbound-only bridge acceptance is required.");
        this.webhookResult(id, e.webhook.fingerprint, true);
        return {state: "accepted"};
    }
    webhookResult(id: string, fingerprint: string, accepted: boolean) {
        const {envelope: e, state} = this.read(id);
        if (!e.webhook || e.webhook.fingerprint !== fingerprint) fail("Mismatched webhook acceptance.");
        if (e.webhook.accepted) return;
        e.webhook.accepted = accepted;
        if (!accepted) { e.webhook.failures++; e.webhook.retryAt = this.now() + Math.min(5000 * 2 ** Math.min(e.webhook.failures - 1, 6), 300000); }
        this.write(id, e, state);
    }
    validate(raw: unknown) {
        const r = object(raw), { envelope: e, state } = this.read(text(r.deliveryId)), d = e.delivery;
        const b = this.bindingFor(d);
        for (const k of ["connectorId", "profile", "chatId", "senderId"] as const) if (r[k] !== b[k]) fail("The relay recipient does not match its binding.");
        if ((r.channel ?? "whatsapp") !== (b.channel ?? "whatsapp") || r.attemptId !== d.attemptId) fail("The relay attempt/channel changed.");
        if (e.webhook && !this.deps.webhooks?.().some(t => t.connectorId === d.connectorId && t.profile === d.profile && t.channel === (d.channel ?? "whatsapp") && t.endpointUrl === e.webhook!.endpoint && (t.mode ?? "push") === e.webhook!.mode)) fail("The webhook transport changed; the old receiver cannot send.");
        const q = this.queue.getById(d.qitemId);
        return {delivery: d, current: ["sending", "indeterminate"].includes(state) && q?.state === "pending" && q.humanIntent !== "update" && !q.humanForwardingStatus && q.humanRequestRevision === d.requestRevision, requestState: q?.state ?? "missing"};
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
        if ((r.channel ?? "whatsapp") !== (b.channel ?? "whatsapp")) fail("The native channel does not match the bound channel.");
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
        return { qitemId: d.qitemId, requestRevision: d.requestRevision, actorSession: d.humanSession, replyId: "hermes-" + digest(d.channel && d.channel !== "whatsapp" ? [d.connectorId, d.profile, d.chatId, d.senderId, text(r.messageId), d.channel] : [d.connectorId, d.profile, d.chatId, d.senderId, text(r.messageId)]), response };
    }
}
