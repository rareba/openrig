/** Deterministic elastic-capacity planning. No provider or fleet mutations. */
export interface FleetPolicy {
  poolId: string;
  enabled: boolean;
  mode: "dry-run" | "active";
  maxWorkers: number;
  slotsPerWorker: number;
  capabilities: string[];
  pollIntervalSeconds: number;
  scaleOutSamples: number;
  idleSeconds: number;
}
export interface FleetWorkerObservation {
  id: string;
  managed: boolean;
  permanent: boolean;
  state: "provisioning" | "ready" | "draining" | "quarantined";
  capabilities: string[];
  slots: number | null;
  active: number | null;
  observedAt: number;
  idleSince: number | null;
  clean: boolean | null;
  pendingHumanReturns: boolean | null;
}
export interface FleetDemand { id: string; capability: string }
export interface FleetSnapshot {
  observedAt: number;
  now: number;
  demands: FleetDemand[];
  workers: FleetWorkerObservation[];
  providerAvailable: boolean;
}
export interface FleetHistory { lastObservedAt: number | null; demandSamples: number }
export interface FleetPlan {
  add: number;
  drainIds: string[];
  reasons: string[];
  demandSamples: number;
  observedAt: number;
  /** Last counted sample, retained across polls that are too close together. */
  lastObservedAt: number | null;
  eligibleDemand: number;
  freeSlots: number;
}
type ObjectValue = Record<string, unknown>;
function record(value: unknown): value is ObjectValue {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function integer(value: unknown, minimum = 0): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;
}
function identifier(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}
function capabilities(value: unknown): value is string[] {
  return Array.isArray(value) && Array.from(value).every(identifier);
}
function validPolicy(value: unknown): value is FleetPolicy {
  if (!record(value)) return false;
  return identifier(value.poolId) && typeof value.enabled === "boolean"
    && ["dry-run", "active"].includes(value.mode as string)
    && integer(value.maxWorkers) && integer(value.slotsPerWorker, 1)
    && capabilities(value.capabilities) && value.capabilities.length > 0
    && integer(value.pollIntervalSeconds, 1) && value.pollIntervalSeconds <= Number.MAX_SAFE_INTEGER / 1000
    && integer(value.scaleOutSamples, 2) && integer(value.idleSeconds, 1)
    && value.idleSeconds <= Number.MAX_SAFE_INTEGER / 1000;
}
/** Validates configuration at the runtime boundary; throws on malformed input. */
export function validateFleetPolicy(value: unknown): FleetPolicy {
  if (!validPolicy(value)) throw new Error("Invalid fleet policy");
  return value;
}
function nullableBoolean(value: unknown): boolean {
  return value === null || typeof value === "boolean";
}
function validWorker(value: unknown): value is FleetWorkerObservation {
  if (!record(value)) return false;
  return identifier(value.id) && typeof value.managed === "boolean" && typeof value.permanent === "boolean"
    && ["provisioning", "ready", "draining", "quarantined"].includes(value.state as string)
    && capabilities(value.capabilities)
    && (value.slots === null || integer(value.slots, 1))
    && (value.active === null || integer(value.active))
    && (value.slots === null || value.active === null || (value.active as number) <= (value.slots as number))
    && integer(value.observedAt) && (value.idleSince === null || integer(value.idleSince))
    && nullableBoolean(value.clean) && nullableBoolean(value.pendingHumanReturns);
}
function validSnapshot(value: unknown): value is FleetSnapshot {
  if (!record(value) || !integer(value.now) || !integer(value.observedAt)
    || typeof value.providerAvailable !== "boolean" || !Array.isArray(value.workers)
    || !Array.from(value.workers).every(validWorker) || !Array.isArray(value.demands)
    || !Array.from(value.demands).every((d) => record(d) && identifier(d.id) && identifier(d.capability))) return false;
  return new Set(value.workers.map((worker) => worker.id)).size === value.workers.length;
}
function validHistory(value: unknown, observedAt: number): value is FleetHistory {
  return record(value) && integer(value.demandSamples)
    && ((value.lastObservedAt === null && value.demandSamples === 0)
      || (integer(value.lastObservedAt) && value.lastObservedAt <= observedAt));
}

/** Maximum matching against worker slot capacities; one issue consumes at most one slot. */
function matchedDemand(demands: Set<string>[], workers: Array<{ capabilities: Set<string>; slots: number }>): number {
  const assigned: number[][] = workers.map(() => []);
  function assign(demand: number, visited: Set<number>): boolean {
    for (let index = 0; index < workers.length; index++) {
      const worker = workers[index]!;
      if (visited.has(index) || ![...demands[demand]!].some((capability) => worker.capabilities.has(capability))) continue;
      visited.add(index);
      const occupants = assigned[index]!;
      if (occupants.length < worker.slots) {
        occupants.push(demand);
        return true;
      }
      for (let slot = 0; slot < occupants.length; slot++) {
        if (assign(occupants[slot]!, visited)) {
          occupants[slot] = demand;
          return true;
        }
      }
    }
    return false;
  }
  return demands.reduce((count, _demand, index) => count + (assign(index, new Set()) ? 1 : 0), 0);
}

/**
 * Proposes actions in both active and dry-run modes; execution must separately honor mode.
 * Invalid or uncertain telemetry produces no actions and resets scale-out sampling.
 * Snapshots must be complete inventories of the pool, including in-flight workers.
 */
export function planFleet(policy: FleetPolicy, snapshot: FleetSnapshot, previous?: FleetHistory): FleetPlan {
  const plan: FleetPlan = { add: 0, drainIds: [], reasons: [], demandSamples: 0,
    observedAt: record(snapshot) && integer(snapshot.observedAt) ? snapshot.observedAt : 0,
    lastObservedAt: null, eligibleDemand: 0, freeSlots: 0 };
  const stop = (reason: string): FleetPlan => { plan.reasons.push(reason); return plan; };
  if (!validPolicy(policy)) return stop("invalid_policy");
  if (!validSnapshot(snapshot)) return stop("invalid_snapshot");
  if (previous !== undefined && !validHistory(previous, snapshot.observedAt)) return stop("invalid_history");
  if (!policy.enabled) return stop("disabled");
  if (!snapshot.providerAvailable) return stop("provider_unavailable");
  const interval = policy.pollIntervalSeconds * 1000;
  if (snapshot.observedAt > snapshot.now || snapshot.now - snapshot.observedAt > interval) return stop("snapshot_not_fresh");
  for (const worker of snapshot.workers) {
    if (worker.observedAt > snapshot.observedAt || snapshot.now - worker.observedAt > interval
      || (worker.idleSince !== null && worker.idleSince > worker.observedAt)) return stop("worker_not_fresh");
    if (worker.slots === null || worker.active === null || worker.clean === null || worker.pendingHumanReturns === null) return stop("unknown_worker_telemetry");
  }

  const supported = new Set(policy.capabilities);
  const eligible = new Map<string, Set<string>>();
  for (const demand of snapshot.demands) {
    if (!supported.has(demand.capability)) continue;
    const alternatives = eligible.get(demand.id) ?? new Set<string>();
    alternatives.add(demand.capability);
    eligible.set(demand.id, alternatives);
  }
  const demands = [...eligible.values()];
  plan.eligibleDemand = demands.length;
  const capacity = snapshot.workers.filter((worker) => worker.state === "ready" || worker.state === "provisioning")
    .map((worker) => ({ id: worker.id, capabilities: new Set(worker.capabilities.filter((c) => supported.has(c))),
      slots: worker.slots! - worker.active! }));
  plan.freeSlots = capacity.filter((worker) => worker.capabilities.size > 0).reduce((total, worker) => total + worker.slots, 0);
  if (!Number.isSafeInteger(plan.freeSlots)) {
    plan.freeSlots = 0;
    return stop("invalid_snapshot");
  }
  const shortage = demands.length - matchedDemand(demands, capacity);
  if (shortage > 0) {
    const last = previous?.lastObservedAt ?? null;
    const elapsed = last === null ? null : snapshot.observedAt - last;
    const freshSample = elapsed === null || elapsed >= interval;
    const continuous = elapsed !== null && elapsed <= interval * 2;
    plan.demandSamples = freshSample
      ? Math.min(policy.scaleOutSamples, (continuous ? previous!.demandSamples : 0) + 1)
      : previous!.demandSamples;
    plan.lastObservedAt = freshSample ? snapshot.observedAt : last;
    const count = snapshot.workers.filter((worker) => worker.managed && !worker.permanent).length;
    const available = Math.max(0, policy.maxWorkers - count);
    if (available === 0) return stop("maximum_reached");
    if (plan.demandSamples < policy.scaleOutSamples || !freshSample) return stop("awaiting_demand_samples");
    plan.add = Math.min(available, Math.ceil(shortage / policy.slotsPerWorker));
    return stop("eligible_demand_exceeds_capacity");
  }

  // An in-flight retirement already owns the one-at-a-time drain allowance.
  if (snapshot.workers.some((worker) => worker.state === "draining")) return stop("drain_in_progress");
  const idleMilliseconds = Math.max(900, policy.idleSeconds) * 1000;
  const candidates = snapshot.workers.filter((worker) => worker.managed && !worker.permanent
    && worker.state === "ready" && worker.active === 0 && worker.clean === true
    && worker.pendingHumanReturns === false && worker.idleSince !== null
    && snapshot.now - worker.idleSince >= idleMilliseconds)
    .sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  for (const worker of candidates) {
    if (matchedDemand(demands, capacity.filter((candidate) => candidate.id !== worker.id)) < demands.length) continue;
    plan.drainIds = [worker.id];
    return stop("idle_surplus_capacity");
  }
  return stop("capacity_sufficient");
}
