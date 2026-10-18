import type Database from "better-sqlite3";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { planFleet, validateFleetPolicy, type FleetPolicy, type FleetSnapshot, type FleetHistory, type FleetPlan } from "./elastic-fleet-policy.js";

export interface ElasticFleetConfiguration { policy: FleetPolicy; provider?: Record<string, unknown> }
export interface FleetOperation {
  operationId: string;
  type: "provision" | "beginDrain" | "remove";
  state: "pending" | "succeeded";
  workerId: string | null;
  count: number;
  /** Absolute desired managed, nonpermanent pool capacity, fixed before IO. */
  targetCapacity: number;
  revision: number;
  snapshot: FleetSnapshot;
  error: string | null;
  createdAt: number;
  updatedAt: number;
}
export interface FleetMutation extends FleetOperation { policy: FleetPolicy; configuration: ElasticFleetConfiguration }
export interface FleetIO {
  validateConfiguration?(configuration: ElasticFleetConfiguration): void;
  observe(policy: FleetPolicy, configuration: ElasticFleetConfiguration): Promise<FleetSnapshot>;
  provision(operation: FleetMutation): Promise<void>;
  beginDrain(operation: FleetMutation): Promise<void>;
  remove(operation: FleetMutation): Promise<void>;
  inspectOperation(operation: FleetOperation, configuration: ElasticFleetConfiguration): Promise<"succeeded" | "pending" | "unknown">;
}
export interface ElasticFleetStatus {
  config: ElasticFleetConfiguration | null;
  revision: number;
  paused: boolean;
  history: FleetHistory;
  plan: FleetPlan | null;
  error: string | null;
  operation: FleetOperation | null;
  reconcileOwner: string | null;
  drainingWorkerIds: string[];
}
interface ConfigRow { config: string | null; revision: number; paused: number; history: string; last_plan: string | null; error: string | null; reconcile_owner: string | null }
interface OperationRow { operation_id: string; type: FleetOperation["type"]; state: FleetOperation["state"]; worker_id: string | null;
  count: number; target_capacity: number; revision: number; snapshot: string; configuration: string; error: string | null; created_at: number; updated_at: number }
interface DrainRow { worker_id: string; pool_id: string; state: "requested" | "draining" | "removed"; begin_operation_id: string | null }
const liveOwners = new Set<string>();
const emptyHistory: FleetHistory = { lastObservedAt: null, demandSamples: 0 };
const policyKeys = ["poolId", "enabled", "mode", "maxWorkers", "slotsPerWorker", "capabilities", "pollIntervalSeconds", "scaleOutSamples", "idleSeconds"];
function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype;
}
function jsonValue(value: unknown, seen = new Set<unknown>()): boolean {
  if (value === null || typeof value === "boolean") return true;
  if (typeof value === "string") return value.length <= 4096 && !/Bearer\s|-----BEGIN .*PRIVATE KEY|:\/\/[^/\s]+:[^/\s]+@/i.test(value);
  if (typeof value === "number") return Number.isFinite(value);
  if ((!record(value) && !Array.isArray(value)) || seen.has(value)) return false;
  seen.add(value);
  const valid = Array.isArray(value)
    ? Array.from({ length: value.length }, (_, index) => Object.hasOwn(value, index) && jsonValue(value[index], seen)).every(Boolean)
    : Object.entries(value).every(([key, item]) => !/secret|token|password|credential|private.?key|authorization/i.test(key) && jsonValue(item, seen));
  seen.delete(value);
  return valid;
}
function configuration(value: unknown): ElasticFleetConfiguration {
  if (!record(value) || !Object.keys(value).every((key) => ["policy", "provider"].includes(key)) || !record(value.policy)
    || !Object.keys(value.policy).every((key) => policyKeys.includes(key))
    || (Object.hasOwn(value, "provider") && !record(value.provider)) || !jsonValue(value)) throw Error("invalid_configuration");
  validateFleetPolicy(value.policy);
  return JSON.parse(JSON.stringify(value)) as ElasticFleetConfiguration;
}
function operation(row: OperationRow): FleetOperation {
  return { operationId: row.operation_id, type: row.type, state: row.state, workerId: row.worker_id, count: row.count,
    targetCapacity: row.target_capacity, revision: row.revision, snapshot: JSON.parse(row.snapshot), error: row.error,
    createdAt: row.created_at, updatedAt: row.updated_at };
}

/** One durable writer; ownership never expires automatically after a process crash. */
export class ElasticFleetService {
  constructor(private readonly db: Database.Database, private readonly io: FleetIO, private readonly now: () => number = Date.now) {}

  private row(): ConfigRow { return this.db.prepare("SELECT * FROM elastic_fleet_config WHERE id = 1").get() as ConfigRow; }
  private latestOperation(pending = false): OperationRow | undefined {
    return this.db.prepare(`SELECT * FROM elastic_fleet_operations ${pending ? "WHERE state = 'pending'" : ""} ORDER BY rowid DESC LIMIT 1`).get() as OperationRow | undefined;
  }
  private drains(): DrainRow[] { return this.db.prepare("SELECT * FROM elastic_fleet_drains WHERE state <> 'removed' ORDER BY worker_id").all() as DrainRow[]; }
  drainingWorkerIds(): string[] { return this.drains().map((drain) => drain.worker_id); }
  status(): ElasticFleetStatus {
    const row = this.row(); const latest = this.latestOperation();
    return { config: row.config ? JSON.parse(row.config) : null, revision: row.revision, paused: row.paused === 1,
      history: JSON.parse(row.history), plan: row.last_plan ? JSON.parse(row.last_plan) : null, error: row.error,
      operation: latest ? operation(latest) : null, reconcileOwner: row.reconcile_owner, drainingWorkerIds: this.drainingWorkerIds() };
  }
  configure(value: ElasticFleetConfiguration): ElasticFleetStatus {
    let config: ElasticFleetConfiguration;
    try { config = configuration(value); this.io.validateConfiguration?.(config); config = configuration(config); }
    catch { throw Error("invalid_configuration"); }
    this.db.transaction(() => {
      const previous = this.row();
      const provision = this.db.prepare("SELECT configuration FROM elastic_fleet_operations WHERE type = 'provision' ORDER BY rowid LIMIT 1").get() as { configuration: string } | undefined;
      const identity: ElasticFleetConfiguration | null = provision ? JSON.parse(provision.configuration) : previous.config ? JSON.parse(previous.config) : null;
      if (identity && (identity.policy.poolId !== config.policy.poolId || !isDeepStrictEqual(identity.provider, config.provider))
        && (provision || this.drains().length > 0 || this.latestOperation(true))) throw Error("pool_busy");
      this.db.prepare("UPDATE elastic_fleet_config SET config = ?, revision = revision + 1, paused = 0, history = ?, last_plan = NULL, error = NULL, updated_at = ? WHERE id = 1")
        .run(JSON.stringify(config), JSON.stringify(emptyHistory), this.now());
    }).immediate();
    return this.status();
  }
  pause(): ElasticFleetStatus {
    this.db.prepare("UPDATE elastic_fleet_config SET paused = 1, revision = revision + 1, updated_at = ? WHERE id = 1").run(this.now());
    return this.status();
  }
  requestDrain(workerId: string): ElasticFleetStatus {
    if (typeof workerId !== "string" || !workerId.trim() || workerId.length > 512) throw Error("invalid_worker_id");
    this.db.transaction(() => {
      const row = this.row(); if (!row.config) throw Error("not_configured");
      const pool = (JSON.parse(row.config) as ElasticFleetConfiguration).policy.poolId;
      this.db.prepare("INSERT INTO elastic_fleet_drains (worker_id, pool_id, state, created_at, updated_at) VALUES (?, ?, 'requested', ?, ?) ON CONFLICT(worker_id) DO NOTHING")
        .run(workerId, pool, this.now(), this.now());
      this.db.prepare("UPDATE elastic_fleet_config SET revision = revision + 1, updated_at = ? WHERE id = 1").run(this.now());
    }).immediate();
    return this.status();
  }
  releaseReconcile(input: { owner: string; operatorConfirmedStopped: true }): ElasticFleetStatus {
    if (input.operatorConfirmedStopped !== true || !input.owner || liveOwners.has(input.owner)) throw Error("owner_not_stopped");
    const result = this.db.prepare("UPDATE elastic_fleet_config SET reconcile_owner = NULL WHERE id = 1 AND reconcile_owner = ?").run(input.owner);
    if (result.changes !== 1) throw Error("owner_mismatch");
    return this.status();
  }
  private overlay(snapshot: FleetSnapshot): FleetSnapshot {
    const draining = new Set(this.drainingWorkerIds());
    return { ...snapshot, now: this.now(), workers: Array.isArray(snapshot.workers)
      ? snapshot.workers.map((worker) => draining.has(worker.id) ? { ...worker, state: "draining" as const } : worker) : snapshot.workers };
  }
  async plan(): Promise<FleetPlan | null> {
    const state = this.status(); if (!state.config) return null;
    let observed: FleetSnapshot;
    try { const copy = configuration(state.config); observed = await this.io.observe(copy.policy, copy); }
    catch { throw Error("observation_failed"); }
    return planFleet(state.config.policy, this.overlay(observed), state.history);
  }
  private error(code: string | null): void {
    this.db.prepare("UPDATE elastic_fleet_config SET error = ?, updated_at = ? WHERE id = 1").run(code, this.now());
  }
  private finish(row: OperationRow): void {
    this.db.transaction(() => {
      this.db.prepare("UPDATE elastic_fleet_operations SET state = 'succeeded', error = NULL, updated_at = ? WHERE operation_id = ?").run(this.now(), row.operation_id);
      if (row.type === "beginDrain") this.db.prepare("UPDATE elastic_fleet_drains SET state = 'draining', updated_at = ? WHERE worker_id = ? AND begin_operation_id = ?")
        .run(this.now(), row.worker_id, row.operation_id);
      if (row.type === "remove") this.db.prepare("UPDATE elastic_fleet_drains SET state = 'removed', updated_at = ? WHERE worker_id = ?").run(this.now(), row.worker_id);
      this.error(null);
    }).immediate();
  }
  private reserve(owner: string, revision: number, config: ElasticFleetConfiguration, snapshot: FleetSnapshot,
    type: FleetOperation["type"], workerId: string | null, count: number): OperationRow | null {
    return this.db.transaction(() => {
      const row = this.row();
      if (row.reconcile_owner !== owner || row.revision !== revision || row.paused || !config.policy.enabled || config.policy.mode !== "active" || this.latestOperation(true)) return null;
      const managed = snapshot.workers.filter((worker) => worker.managed && !worker.permanent).length;
      const target = type === "provision" ? managed + count : type === "remove" ? Math.max(0, managed - 1) : managed;
      const id = randomUUID(); const now = this.now();
      if (type === "beginDrain") {
        this.db.prepare("INSERT INTO elastic_fleet_drains (worker_id, pool_id, state, begin_operation_id, created_at, updated_at) VALUES (?, ?, 'requested', ?, ?, ?) ON CONFLICT(worker_id) DO UPDATE SET begin_operation_id = excluded.begin_operation_id, updated_at = excluded.updated_at")
          .run(workerId, config.policy.poolId, id, now, now);
      }
      this.db.prepare("INSERT INTO elastic_fleet_operations (operation_id, type, state, worker_id, count, target_capacity, revision, snapshot, configuration, created_at, updated_at) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)")
        .run(id, type, workerId, count, target, revision, JSON.stringify({ observedAt: snapshot.observedAt, now: snapshot.now, providerAvailable: snapshot.providerAvailable,
          demands: snapshot.demands.map(({ id, capability }) => ({ id, capability })),
          workers: snapshot.workers.map(({ id, managed, permanent, state, capabilities, slots, active, observedAt, idleSince, clean, pendingHumanReturns }) =>
            ({ id, managed, permanent, state, capabilities, slots, active, observedAt, idleSince, clean, pendingHumanReturns })) }), JSON.stringify(config), now, now);
      return this.latestOperation(true)!;
    }).immediate();
  }
  async reconcile(): Promise<ElasticFleetStatus> {
    const owner = `${process.pid}:${randomUUID()}`;
    if (this.db.prepare("UPDATE elastic_fleet_config SET reconcile_owner = ? WHERE id = 1 AND reconcile_owner IS NULL").run(owner).changes !== 1) return this.status();
    liveOwners.add(owner);
    try { await this.reconcileOwned(owner); }
    finally {
      liveOwners.delete(owner);
      this.db.prepare("UPDATE elastic_fleet_config SET reconcile_owner = NULL WHERE id = 1 AND reconcile_owner = ?").run(owner);
    }
    return this.status();
  }
  private async reconcileOwned(owner: string): Promise<ElasticFleetStatus> {
    const pending = this.latestOperation(true);
    if (pending) {
      try {
        const inspected = await this.io.inspectOperation(operation(pending), JSON.parse(pending.configuration));
        if (inspected === "succeeded") this.finish(pending);
        else this.error(inspected === "pending" ? "operation_pending" : "operation_unknown");
      } catch { this.error("inspection_failed"); }
      return this.status();
    }
    const state = this.status(); const config = state.config;
    if (!config || state.paused || !config.policy.enabled) return this.status();
    let observed: FleetSnapshot;
    try { const copy = configuration(config); observed = await this.io.observe(copy.policy, copy); }
    catch { this.error("observation_failed"); return this.status(); }
    const snapshot = this.overlay(observed);
    const plan = planFleet(config.policy, snapshot, state.history);
    const saved = this.db.prepare("UPDATE elastic_fleet_config SET history = ?, last_plan = ?, error = NULL, updated_at = ? WHERE id = 1 AND revision = ? AND reconcile_owner = ? AND paused = 0")
      .run(JSON.stringify({ lastObservedAt: plan.lastObservedAt, demandSamples: plan.demandSamples }), JSON.stringify(plan), this.now(), state.revision, owner);
    if (!saved.changes || config.policy.mode !== "active") return this.status();
    // The planner validates the complete native inventory before retirement can proceed.
    if (plan.reasons.some((reason) => ["invalid_snapshot", "invalid_history", "snapshot_not_fresh", "worker_not_fresh", "unknown_worker_telemetry", "provider_unavailable", "invalid_policy"].includes(reason))) return this.status();
    let type: FleetOperation["type"] | null = null; let workerId: string | null = null; let count = 0;
    const drain = this.drains()[0];
    if (drain) {
      const native = observed.workers.find((worker) => worker.id === drain.worker_id);
      if (drain.pool_id === config.policy.poolId && native?.managed && !native.permanent
        && ["ready", "draining"].includes(native.state)) {
        workerId = native.id;
        if (drain.state === "requested") type = "beginDrain";
        else if (native.active === 0 && native.clean === true && native.pendingHumanReturns === false && drain.begin_operation_id) {
          const begin = this.db.prepare("SELECT state, updated_at FROM elastic_fleet_operations WHERE operation_id = ? AND type = 'beginDrain' AND worker_id = ?").get(drain.begin_operation_id, native.id) as { state: string; updated_at: number } | undefined;
          if (begin?.state === "succeeded" && snapshot.observedAt > begin.updated_at && native.observedAt > begin.updated_at) type = "remove";
        }
      }
    }
    if (!type && plan.add > 0) { type = "provision"; workerId = null; count = plan.add; }
    else if (!drain && !type && plan.drainIds[0]) { type = "beginDrain"; workerId = plan.drainIds[0]; }
    if (!type) return this.status();
    const reserved = this.reserve(owner, state.revision, config, snapshot, type, workerId, count);
    if (!reserved) return this.status();
    try {
      await this.io[type]({ ...operation(reserved), policy: config.policy, configuration: config });
      this.finish(reserved);
    } catch {
      this.db.prepare("UPDATE elastic_fleet_operations SET error = 'mutation_uncertain', updated_at = ? WHERE operation_id = ?").run(this.now(), reserved.operation_id);
      this.error("mutation_uncertain");
    }
    return this.status();
  }
}
