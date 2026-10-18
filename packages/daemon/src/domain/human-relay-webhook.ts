import { createHash, createHmac } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { HumanRelayDelivery, HumanRelayService } from "./human-relay.js";
import { validateOutboundUrl } from "./mission-control/outbound-url-validator.js";

export type HumanRelayChannel = "whatsapp" | "slack" | "teams";
export interface HumanRelayWebhookTarget {
  connectorId: string;
  profile: string;
  channel: HumanRelayChannel;
  endpointUrl: string;
  secretFile?: string;
  mode?: "push" | "pull-bridge";
}
export function relayCanonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(relayCanonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, v]) => `${JSON.stringify(key)}:${relayCanonicalJson(v)}`).join(",")}}`;
  return JSON.stringify(value);
}
export const relayFingerprint = (value: unknown) => createHash("sha256").update(relayCanonicalJson(value)).digest("hex");
export function loadHumanRelayWebhooks(home: string): HumanRelayWebhookTarget[] {
  const path = join(home, "human-relay.json");
  if (!existsSync(path)) return [];
  const config = JSON.parse(readFileSync(path, "utf8"));
  if (!config.webhooks) return [];
  if (config.version !== 1 || !Array.isArray(config.webhooks) || config.webhooks.length > 10) throw Error("Invalid human relay webhook configuration.");
  const seen = new Set<string>();
  return config.webhooks.map((raw: Record<string, unknown>) => {
    const channel = raw.channel ?? "whatsapp";
    if (!["whatsapp", "slack", "teams"].includes(String(channel))) throw Error("Unsupported human relay channel.");
    for (const key of ["connectorId", "profile", "endpointUrl"]) if (typeof raw[key] !== "string" || !raw[key] || String(raw[key]).length > 4096) throw Error("Invalid human relay webhook target.");
    if (raw.mode !== undefined && raw.mode !== "push" && raw.mode !== "pull-bridge") throw Error("Unsupported relay transport mode.");
    if (raw.mode !== "pull-bridge" && (typeof raw.secretFile !== "string" || !raw.secretFile)) throw Error("Push webhooks require a local secret file.");
    const result = { mode: raw.mode ?? "push", connectorId: raw.connectorId, profile: raw.profile, channel, endpointUrl: raw.endpointUrl, secretFile: raw.secretFile } as HumanRelayWebhookTarget;
    const url = validateOutboundUrl(result.endpointUrl);
    if (!url.valid || url.parsedUrl?.username || url.parsedUrl?.password || url.parsedUrl?.hash) throw Error("Configure a trusted HTTP(S) relay URL without embedded credentials or fragment.");
    const key = JSON.stringify([result.connectorId, result.profile, channel]);
    if (seen.has(key)) throw Error("Duplicate human relay webhook target.");
    seen.add(key);
    return result;
  });
}
export interface HumanRelayWebhookEnvelope {
  delivery: HumanRelayDelivery;
  fingerprint: string;
}
/** Deterministic outbox drain. A receiver's durable acceptance never fabricates a platform receipt. */
export class HumanRelayWebhookDispatcher {
  constructor(private service: HumanRelayService, private deps: {
    targets: () => HumanRelayWebhookTarget[];
    fetchImpl?: typeof fetch;
    readSecret?: (path: string) => string;
    now?: () => number;
  }) {}
  status() { return { config: { pollIntervalSeconds: 5 } }; }
  async reconcile(): Promise<{ error: string | null }> {
    let failed = false;
    for (const target of this.deps.targets().filter(t => t.mode !== "pull-bridge")) {
      for (const envelope of this.service.webhookBatch(target)) {
        const { delivery, fingerprint } = envelope;
        try {
          if (!target.secretFile) throw Error("Missing relay secret file.");
          const secret = (this.deps.readSecret ?? (path => readFileSync(path, "utf8").trim()))(target.secretFile);
          if (!secret || secret.startsWith("whsec_")) throw Error("Configure a nonempty plain relay route secret.");
          const body = relayCanonicalJson({ schema: "openrig.human-relay.v1", type: "question.created", deliveryId: delivery.deliveryId, fingerprint, delivery });
          const timestamp = String(Math.floor((this.deps.now?.() ?? Date.now()) / 1000));
          const signature = createHmac("sha256", secret).update(`${delivery.deliveryId}.${timestamp}.${body}`).digest("base64");
          const response = await (this.deps.fetchImpl ?? fetch)(target.endpointUrl, { method: "POST", redirect: "error", signal: AbortSignal.timeout(12000), headers: { "Content-Type": "application/json", "webhook-id": delivery.deliveryId, "webhook-timestamp": timestamp, "webhook-signature": `v1,${signature}` }, body });
          // A generic 2xx can mean filtered/ignored. Only this explicit receipt proves persistence.
          if (!response.ok || Number(response.headers.get("content-length") ?? 0) > 65536) throw Error("Relay did not acknowledge durable acceptance.");
          const reader = response.body?.getReader(); let size = 0, text = "";
          if (!reader) throw Error("Missing relay acceptance.");
          const decoder = new TextDecoder();
          while (true) { const chunk = await reader.read(); if (chunk.done) break; size += chunk.value.byteLength; if (size > 65536) { await reader.cancel(); throw Error("Oversized relay acceptance."); } text += decoder.decode(chunk.value, { stream: true }); }
          text += decoder.decode();
          const ack = JSON.parse(text);
          if (ack.schema !== "openrig.human-relay.v1" || ack.status !== "accepted" || ack.deliveryId !== delivery.deliveryId || ack.fingerprint !== fingerprint) throw Error("Relay acceptance did not match the immutable delivery.");
          this.service.webhookResult(delivery.deliveryId, fingerprint, true);
        } catch {
          // No endpoint, message body, credentials or provider response reaches logs.
          failed = true;
          this.service.webhookResult(delivery.deliveryId, fingerprint, false);
        }
      }
    }
    return { error: failed ? "Human relay webhook acceptance is unconfirmed; durable deliveries will retry." : null };
  }
}
