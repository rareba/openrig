import { afterEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFullTestDb } from "./helpers/test-app.js";
import { migrate } from "../src/db/migrate.js";
import { elasticFleetSchema } from "../src/db/migrations/116_elastic_fleet.js";
import { ElasticFleetService, type ElasticFleetConfiguration, type FleetIO, type FleetOperation } from "../src/domain/elastic-fleet.js";
import type { FleetSnapshot, FleetWorkerObservation } from "../src/domain/elastic-fleet-policy.js";

const config: ElasticFleetConfiguration = { policy: { poolId: "elastic", enabled: true, mode: "active", maxWorkers: 3,
  slotsPerWorker: 1, capabilities: ["code"], pollIntervalSeconds: 10, scaleOutSamples: 2, idleSeconds: 900 },
  provider: { resourceGroup: "test" } };
let time = 1_000_000;
const worker = (patch: Partial<FleetWorkerObservation> = {}): FleetWorkerObservation => ({ id: "vm1", managed: true,
  permanent: false, state: "ready", capabilities: ["code"], slots: 1, active: 0, observedAt: time,
  idleSince: 0, clean: true, pendingHumanReturns: false, ...patch });
const snapshot = (workers: FleetWorkerObservation[] = [], demand = true): FleetSnapshot => ({ now: time,
  observedAt: time, workers, demands: demand ? [{ id: "issue1", capability: "code" }] : [], providerAvailable: true });
const cleanup: Array<() => void> = [];
afterEach(() => { for (const close of cleanup.splice(0)) close(); time = 1_000_000; });
function database() { const db = createFullTestDb(); migrate(db, [elasticFleetSchema]); cleanup.push(() => db.close()); return db; }
function boundary() {
  const calls: FleetOperation[] = [];
  let observed = snapshot();
  let inspection: "succeeded" | "pending" | "unknown" = "unknown";
  const io: FleetIO = { observe: async () => observed,
    provision: async (operation) => { calls.push(operation); }, beginDrain: async (operation) => { calls.push(operation); },
    remove: async (operation) => { calls.push(operation); }, inspectOperation: async () => inspection };
  return { io, calls, set: (value: FleetSnapshot) => { observed = value; }, inspect: (value: typeof inspection) => { inspection = value; } };
}

describe("durable elastic fleet lifecycle", () => {
  it("reopens configuration and counted history without counting close polls", async () => {
    const db = database(); const b = boundary(); const first = new ElasticFleetService(db, b.io, () => time);
    first.configure(config); await first.reconcile(); time += 5_000; b.set(snapshot()); await first.reconcile();
    const reopened = new ElasticFleetService(db, b.io, () => time);
    expect(reopened.status().config).toEqual(config);
    expect(reopened.status().history).toEqual({ lastObservedAt: 1_000_000, demandSamples: 1 });
    time += 5_000; b.set(snapshot()); await reopened.reconcile();
    expect(b.calls).toHaveLength(1); expect(b.calls[0]).toMatchObject({ type: "provision", count: 1, targetCapacity: 1 });
  });

  it("commits intent and absolute target before provider IO, retaining ambiguous failure across restart", async () => {
    const db = database(); const b = boundary();
    b.io.provision = async (operation) => {
      expect(new ElasticFleetService(db, b.io).status().operation).toMatchObject({ operationId: operation.operationId, state: "pending", targetCapacity: 1 });
      b.calls.push(operation); throw new Error("Bearer super-secret provider response");
    };
    const service = new ElasticFleetService(db, b.io, () => time); service.configure(config);
    await service.reconcile(); time += 10_000; b.set(snapshot());
    expect((await service.reconcile()).reconcileOwner).toBeNull();
    const reopened = new ElasticFleetService(db, b.io, () => time);
    expect(reopened.status().error).toBe("mutation_uncertain");
    expect(JSON.stringify(reopened.status())).not.toContain("super-secret");
    await reopened.reconcile(); expect(b.calls).toHaveLength(1);
    expect(reopened.status().operation?.state).toBe("pending");
    b.inspect("succeeded"); await reopened.reconcile(); expect(reopened.status().operation?.state).toBe("succeeded");
  });

  it("dry-run proposes capacity and performs no provider mutations, including a requested drain", async () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time);
    service.configure({ ...config, policy: { ...config.policy, mode: "dry-run" } });
    await service.reconcile(); time += 10_000; b.set(snapshot());
    expect((await service.plan())?.add).toBe(1);
    service.requestDrain("vm1"); await service.reconcile(); expect(b.calls).toEqual([]);
  });

  it("serializes independent SQLite connections and fences a configure/pause racing observation", async () => {
    const seed = database(); const dir = mkdtempSync(join(tmpdir(), "fleet-")); const file = join(dir, "db.sqlite");
    await seed.backup(file); const secondDb = new Database(file); const firstDb = new Database(file);
    cleanup.push(() => { secondDb.close(); firstDb.close(); rmSync(dir, { recursive: true }); });
    const b = boundary(); let finish!: (value: FleetSnapshot) => void;
    b.io.observe = () => new Promise((resolve) => { finish = resolve; });
    const first = new ElasticFleetService(firstDb, b.io, () => time); const second = new ElasticFleetService(secondDb, b.io, () => time);
    first.configure(config); const running = first.reconcile();
    expect(second.status().reconcileOwner).toBeTruthy();
    await second.reconcile(); second.pause(); finish(snapshot([worker()], false)); await running;
    expect(b.calls).toEqual([]); expect(first.status().paused).toBe(true);
    expect(first.status().history.demandSamples).toBe(0);
  });

  it("keeps a crashed owner until exact explicit recovery and inspects pending intent without replay", async () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time); service.configure(config);
    db.prepare("UPDATE elastic_fleet_config SET reconcile_owner = ? WHERE id = 1").run("dead-owner");
    await service.reconcile(); expect(b.calls).toEqual([]);
    expect(() => service.releaseReconcile({ owner: "other", operatorConfirmedStopped: true })).toThrow();
    expect(() => service.releaseReconcile({ owner: "dead-owner", operatorConfirmedStopped: false as true })).toThrow();
    service.releaseReconcile({ owner: "dead-owner", operatorConfirmedStopped: true });
    expect(service.status().reconcileOwner).toBeNull();
  });

  it("excludes requested drains durably before beginDrain and requires a confirmed drain before exact removal", async () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time); service.configure(config);
    b.set(snapshot([worker()], false));
    b.io.beginDrain = async (operation) => { expect(service.drainingWorkerIds()).toEqual(["vm1"]); b.calls.push(operation); throw Error("lost reply"); };
    service.requestDrain("vm1"); await service.reconcile();
    await service.reconcile(); expect(b.calls.map((c) => c.type)).toEqual(["beginDrain"]);
    b.inspect("succeeded"); await service.reconcile();
    expect((await service.plan())?.freeSlots).toBe(0);
    await service.reconcile();
    expect(b.calls.map((c) => c.type)).toEqual(["beginDrain", "remove"]);
    expect(b.calls[1]).toMatchObject({ workerId: "vm1", targetCapacity: 0 });
    expect(service.drainingWorkerIds()).toEqual([]);
  });

  it.each([
    ["absent", []], ["manual", [worker({ managed: false })]], ["permanent", [worker({ permanent: true })]],
    ["busy", [worker({ active: 1 })]], ["dirty", [worker({ clean: false })]],
    ["human returns", [worker({ pendingHumanReturns: true })]], ["unknown", [worker({ active: null })]],
    ["stale", [worker({ observedAt: 980_000 })]], ["future", [worker({ observedAt: 1_000_001 })]],
    ["quarantined", [worker({ state: "quarantined" })]],
  ])("never removes a drained worker with %s native evidence", async (_label, workers) => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time); service.configure(config);
    b.set(snapshot([worker()], false)); service.requestDrain("vm1"); await service.reconcile();
    b.set(snapshot(workers as FleetWorkerObservation[], false)); await service.reconcile();
    expect(b.calls.map((c) => c.type)).toEqual(["beginDrain"]);
    expect(service.drainingWorkerIds()).toEqual(["vm1"]);
  });

  it("pause blocks new mutations while allowing an already reserved operation to finish", async () => {
    const db = database(); const b = boundary(); let finish!: () => void;
    b.io.beginDrain = async (operation) => { b.calls.push(operation); await new Promise<void>((resolve) => { finish = resolve; }); };
    const service = new ElasticFleetService(db, b.io, () => time); service.configure(config); b.set(snapshot([worker()], false));
    service.requestDrain("vm1"); const pending = service.reconcile();
    await Promise.resolve(); service.pause(); finish(); await pending; await service.reconcile();
    expect(b.calls.map((c) => c.type)).toEqual(["beginDrain"]); expect(service.status().paused).toBe(true);
  });

  it("rejects non-JSON, sparse, unknown and secret configuration without recording adapter errors", () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time);
    for (const invalid of [ { ...config, extra: "value" }, { ...config, provider: { token: "secret" } },
      { ...config, provider: { resourceGroup: undefined } }, { ...config, provider: { count: NaN } },
      { ...config, provider: { values: new Array(2) } }, { ...config, policy: { ...config.policy, capabilities: new Array(2) } } ]) {
      expect(() => service.configure(invalid)).toThrow("invalid_configuration");
    }
    b.io.validateConfiguration = () => { throw new Error("private adapter details"); };
    expect(() => service.configure(config)).toThrow("invalid_configuration");
    expect(JSON.stringify(service.status())).not.toContain("private adapter");
  });

  it("passes provider configuration to observation and fences configuration changes during observation", async () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time);
    let observedConfig: ElasticFleetConfiguration | undefined;
    b.io.observe = async (_policy, fullConfig) => { observedConfig = fullConfig; service.configure({ ...config, policy: { ...config.policy, enabled: false } }); return snapshot([worker()], false); };
    service.configure(config); await service.reconcile(); expect(observedConfig).toEqual(config); expect(b.calls).toEqual([]);
  });
  it("stores only the native observation contract, excluding adapter diagnostic secrets", async () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time);
    service.configure(config); b.set({ ...snapshot([worker()], false), diagnostics: "secret-adapter-payload" } as FleetSnapshot);
    await service.reconcile();
    expect(JSON.stringify(service.status())).not.toContain("secret-adapter-payload");
    expect(JSON.stringify(db.prepare("SELECT snapshot FROM elastic_fleet_operations").all())).not.toContain("secret-adapter-payload");
  });

  it("rejects secrets injected by configuration validation before persisting", () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time);
    b.io.validateConfiguration = (value) => { value.provider!.token = "adapter-secret"; };
    expect(() => service.configure(config)).toThrow("invalid_configuration");
    expect(service.status().config).toBeNull();
  });

  it("sanitizes observation errors in read-only planning without changing history", async () => {
    const db = database(); const b = boundary(); const service = new ElasticFleetService(db, b.io, () => time);
    service.configure(config); b.io.observe = async () => { throw Error("secret-access-token"); };
    await expect(service.plan()).rejects.toThrow("observation_failed");
    expect(service.status().history).toEqual({ lastObservedAt: null, demandSamples: 0 });
  });

  it("refuses explicit unlock of a live writer and fences a drain request racing its observation", async () => {
    const db = database(); const b = boundary(); const first = new ElasticFleetService(db, b.io, () => time);
    const second = new ElasticFleetService(db, b.io, () => time); let finish!: (value: FleetSnapshot) => void;
    b.io.observe = () => new Promise((resolve) => { finish = resolve; });
    first.configure(config); const running = first.reconcile();
    const owner = second.status().reconcileOwner!;
    expect(() => second.releaseReconcile({ owner, operatorConfirmedStopped: true })).toThrow("owner_not_stopped");
    second.requestDrain("vm1"); finish(snapshot([worker()], false)); await running;
    expect(b.calls).toEqual([]); expect(first.drainingWorkerIds()).toEqual(["vm1"]);
  });

});
