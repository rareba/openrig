import type { HumanResponseInput, HumanResponseReceipt, QueueItem } from './queue-repository.js';

export const ELASTIC_HUMAN_FORWARD_PREFIX = 'elastic-human-forward-';
export const ELASTIC_HUMAN_FORWARD_GATE = 'external:elastic-human-forward:';
export interface ElasticHumanWorker {
  workerId: string;
  vmId: string;
  nativeHostId: string;
  fleetEpoch: string;
  manifestDigest: string;
}
/** Trusted synchronous coordinator composition only. Never populated from queue tags or HTTP input. */
export interface ElasticHumanProxyAuthority {
  currentWorker(workerId: string): ElasticHumanWorker | null;
  isLocalManager(session: string): boolean;
  isOriginManager(worker: ElasticHumanWorker, session: string): boolean;
  isHumanBinding(coordinatorHuman: string, originHuman: string): boolean;
}
export interface ElasticHumanProxyCreate {
  worker: ElasticHumanWorker;
  /** Authoritative native readback from this exact enrolled worker. */
  origin: QueueItem;
  coordinatorManager: string;
  coordinatorHuman: string;
}
export interface ElasticHumanProxyMapping {
  version: 1;
  mappingId: string;
  worker: ElasticHumanWorker;
  originQitemId: string;
  originRevision: string;
  originManager: string;
  originHuman: string;
  proxyQitemId: string;
  coordinatorManager: string;
  coordinatorHuman: string;
  sequence: number;
}
/** Immutable transaction payload. Task5b forwards originInput verbatim, after incarnation validation.
 * No endpoint or credential is supplied here. A relay hop must preserve/degrade provenance, never upgrade it. */
export interface ElasticHumanForward {
  version: 1;
  intentId: string;
  mapping: ElasticHumanProxyMapping;
  proxyRevision: string;
  proxyReplyId: string;
  localReceiptQitemId: string;
  fingerprint: string;
  originInput: HumanResponseInput;
}
/** Trusted consumer supplies a validated native receipt AND authoritative exact-question readback.
 * This proves native application only, never worker execution. */
export interface NativeHumanResponseCommit {
  qitemId: string; actorSession: string; requestRevision: string; response: HumanResponseInput["response"];
  fingerprint: string; receipt: HumanResponseReceipt;
}
export interface ElasticHumanForwardSettle {
  intentId: string;
  worker: ElasticHumanWorker;
  commit: NativeHumanResponseCommit;
  origin: QueueItem;
}
export interface ElasticHumanProxyReconcile {
  proxyQitemId: string;
  worker: ElasticHumanWorker;
  origin: QueueItem;
  /** remaining requires the preceding verified receipt; changed retains a direct-origin winner. */
  disposition: 'remaining' | 'origin-changed';
}
