import { describe, expect, it } from "vitest";
import {
  planFleet,
  validateFleetPolicy,
  type FleetHistory,
  type FleetPolicy,
  type FleetSnapshot,
  type FleetWorkerObservation,
} from "../src/domain/elastic-fleet-policy.js";

const NOW = 2_000_000;
const policy: FleetPolicy = {
  poolId: "azure", enabled: true, mode: "active", maxWorkers: 7,
  slotsPerWorker: 2, capabilities: ["implement", "review"],
  pollIntervalSeconds: 60, scaleOutSamples: 2, idleSeconds: 900,
};
function worker(overrides: Partial<FleetWorkerObservation> = {}): FleetWorkerObservation {
  return { id: "worker-a", managed: true, permanent: false, state: "ready",
    capabilities: ["implement", "review"], slots: 2, active: 0,
    observedAt: NOW, idleSince: NOW - 900_000, clean: true,
    pendingHumanReturns: false, ...overrides };
}
function snapshot(overrides: Partial<FleetSnapshot> = {}): FleetSnapshot {
  return { observedAt: NOW, now: NOW, demands: [], workers: [], providerAvailable: true, ...overrides };
}
const prior: FleetHistory = { lastObservedAt: NOW - 60_000, demandSamples: 1 };
const demand = [{ id: "issue-1", capability: "implement" }];
function noActions(plan: ReturnType<typeof planFleet>) {
  expect(plan.add).toBe(0);
  expect(plan.drainIds).toEqual([]);
}

describe("planFleet", () => {
  it("disabled policies cannot propose additions or drains and reset sampling", () => {
    const result = planFleet({ ...policy, enabled: false }, snapshot({ demands: demand, workers: [worker()] }), prior);
    noActions(result);
    expect(result.demandSamples).toBe(0);
    expect(result.reasons).toContain("disabled");
  });

  it("dry-run returns the same proposed additions as active mode", () => {
    const input = snapshot({ demands: demand });
    expect(planFleet({ ...policy, mode: "dry-run" }, input, prior).add).toBe(1);
    expect(planFleet(policy, input, prior).add).toBe(1);
  });

  it("filters unsupported demand and deduplicates issue IDs and capabilities", () => {
    const result = planFleet({ ...policy, capabilities: ["review", "implement", "review"] }, snapshot({ demands: [
      ...demand, ...demand, { id: "issue-1", capability: "review" },
      { id: "issue-2", capability: "review" }, { id: "outside", capability: "gpu" },
    ] }), prior);
    expect(result.eligibleDemand).toBe(2);
    expect(result.add).toBe(1);
  });

  it("counts free slots once when a worker supports multiple capabilities", () => {
    const result = planFleet(policy, snapshot({ workers: [worker({ slots: 1 })], demands: [
      ...demand, { id: "issue-2", capability: "review" },
    ] }), prior);
    expect(result.freeSlots).toBe(1);
    expect(result.add).toBe(1);
  });

  it("matches specific workers before general workers without inventing spare capability", () => {
    const result = planFleet(policy, snapshot({ workers: [worker({ slots: 1 }), worker({ id: "specific", slots: 1, capabilities: ["implement"] })], demands: [
      ...demand, { id: "issue-2", capability: "review" },
    ] }), prior);
    expect(result.freeSlots).toBe(2);
    noActions(result);
  });

  it("does not let incompatible free capacity satisfy eligible demand", () => {
    const result = planFleet(policy, snapshot({ workers: [worker({ capabilities: ["review"] })], demands: demand }), prior);
    expect(result.add).toBe(1);
  });

  it("reserves provisioning worker slots so repeated polls cannot overprovision", () => {
    const result = planFleet(policy, snapshot({ demands: demand, workers: [worker({ state: "provisioning", idleSince: null })] }), prior);
    expect(result.freeSlots).toBe(2);
    noActions(result);
  });

  it("counts provisioning, draining and quarantined workers toward the elastic ceiling", () => {
    const result = planFleet({ ...policy, maxWorkers: 3 }, snapshot({ demands: Array.from({ length: 10 }, (_, i) => ({ id: `issue-${i}`, capability: "implement" })), workers: [
      worker({ state: "provisioning" }), worker({ id: "draining", state: "draining" }), worker({ id: "quarantined", state: "quarantined" }),
    ] }), prior);
    noActions(result);
    expect(result.reasons).toContain("maximum_reached");
  });

  it("caps additions at remaining elastic capacity without charging permanent hosts", () => {
    const result = planFleet({ ...policy, maxWorkers: 2 }, snapshot({ demands: Array.from({ length: 20 }, (_, i) => ({ id: `issue-${i}`, capability: "implement" })), workers: [
      worker({ active: 2 }), worker({ id: "permanent", permanent: true, active: 2 }), worker({ id: "outside", managed: false, active: 2 }),
    ] }), prior);
    expect(result.add).toBe(1);
  });

  it("requires two samples spaced by the configured interval", () => {
    const first = planFleet(policy, snapshot({ demands: demand }));
    expect(first.add).toBe(0);
    expect(first.demandSamples).toBe(1);
    expect(first.lastObservedAt).toBe(NOW);
    const close = planFleet(policy, snapshot({ observedAt: NOW + 59_999, now: NOW + 59_999, demands: demand }), { lastObservedAt: first.lastObservedAt, demandSamples: first.demandSamples });
    expect(close.add).toBe(0);
    expect(close.lastObservedAt).toBe(NOW);
    const second = planFleet(policy, snapshot({ observedAt: NOW + 60_000, now: NOW + 60_000, demands: demand }), { lastObservedAt: close.lastObservedAt, demandSamples: close.demandSamples });
    expect(second.add).toBe(1);
    expect(second.demandSamples).toBe(2);
  });

  it("replayed samples cannot increase demand history", () => {
    const result = planFleet(policy, snapshot({ demands: demand }), { lastObservedAt: NOW, demandSamples: 1 });
    expect(result.demandSamples).toBe(1);
    expect(result.add).toBe(0);
  });

  it("resets sustained-demand history when demand is satisfied", () => {
    expect(planFleet(policy, snapshot({ demands: demand, workers: [worker()] }), prior).demandSamples).toBe(0);
  });

  it("does not combine samples separated by a telemetry gap", () => {
    const result = planFleet(policy, snapshot({ demands: demand }), { lastObservedAt: NOW - 120_001, demandSamples: 1 });
    expect(result.demandSamples).toBe(1);
    expect(result.add).toBe(0);
  });

  it.each([
    ["stale snapshot", { observedAt: NOW - 60_001 }],
    ["future snapshot", { observedAt: NOW + 1 }],
    ["provider unavailable", { providerAvailable: false }],
    ["stale worker", { workers: [worker({ observedAt: NOW - 60_001 })] }],
    ["future worker", { workers: [worker({ observedAt: NOW + 1 })] }],
  ] as const)("fails closed for %s", (_label, overrides) => {
    const result = planFleet(policy, snapshot({ demands: demand, ...overrides }), prior);
    noActions(result);
    expect(result.demandSamples).toBe(0);
  });

  it("drains one idle host at a time in deterministic order", () => {
    const result = planFleet(policy, snapshot({ workers: [worker({ id: "z" }), worker({ id: "a" })] }));
    expect(result.drainIds).toEqual(["a"]);
    expect(result.add).toBe(0);
    expect(planFleet({ ...policy, mode: "dry-run" }, snapshot({ workers: [worker()] })).drainIds).toEqual(["worker-a"]);
  });

  it("does not start a second drain while another worker is draining", () => {
    noActions(planFleet(policy, snapshot({ workers: [worker(), worker({ id: "other", state: "draining" })] })));
  });

  it("preserves enough compatible capacity for demand when considering an idle drain", () => {
    noActions(planFleet(policy, snapshot({ workers: [worker()], demands: demand })));
    expect(planFleet(policy, snapshot({ workers: [worker({ id: "a", capabilities: ["implement"] }), worker({ id: "z", capabilities: ["review"] })], demands: demand })).drainIds).toEqual(["z"]);
  });

  it.each([
    ["unmanaged", { managed: false }], ["permanent", { permanent: true }],
    ["active work", { active: 1 }], ["unknown activity", { active: null }],
    ["dirty workspace", { clean: false }], ["unknown workspace", { clean: null }],
    ["human return", { pendingHumanReturns: true }], ["unknown human state", { pendingHumanReturns: null }],
    ["provisioning", { state: "provisioning" }], ["quarantined", { state: "quarantined" }],
    ["insufficient idle time", { idleSince: NOW - 899_999 }], ["unknown idle time", { idleSince: null }],
    ["future idle time", { idleSince: NOW + 1 }], ["unknown slots", { slots: null }],
  ] as const)("never drains %s", (_label, overrides) => {
    noActions(planFleet(policy, snapshot({ workers: [worker(overrides)] })));
  });

  it("never shortens the fifteen-minute retirement floor", () => {
    noActions(planFleet({ ...policy, idleSeconds: 1 }, snapshot({ workers: [worker({ idleSince: NOW - 10_000 })] })));
  });

  it.each([
    ["NaN maximum", { maxWorkers: NaN }], ["infinite slots", { slotsPerWorker: Infinity }],
    ["negative maximum", { maxWorkers: -1 }], ["fractional slots", { slotsPerWorker: 1.5 }],
    ["zero slots", { slotsPerWorker: 0 }], ["zero interval", { pollIntervalSeconds: 0 }],
    ["fractional samples", { scaleOutSamples: 1.5 }], ["empty pool", { poolId: "" }],
    ["unknown mode", { mode: "unknown" }], ["invalid flag", { enabled: "yes" }],
    ["invalid capabilities", { capabilities: ["implement", 1] }],
  ])("rejects malformed policy: %s", (_label, overrides) => {
    const result = planFleet({ ...policy, ...overrides } as FleetPolicy, snapshot({ demands: demand }), prior);
    noActions(result);
    expect(result.reasons).toContain("invalid_policy");
  });

  it.each([
    ["unknown worker state", { workers: [worker({ state: "alien" as "ready" })] }],
    ["overcommitted worker", { workers: [worker({ active: 3 })] }],
    ["duplicate worker identity", { workers: [worker(), worker()] }],
    ["fractional worker slots", { workers: [worker({ slots: 1.5 })] }],
    ["negative activity", { workers: [worker({ active: -1 })] }],
    ["invalid worker capability", { workers: [worker({ capabilities: [1] as unknown as string[] })] }],
    ["invalid worker ownership", { workers: [worker({ managed: null as unknown as boolean })] }],
    ["invalid clean state", { workers: [worker({ clean: "yes" as unknown as boolean })] }],
    ["invalid demand", { demands: [{ id: "", capability: "implement" }] }],
    ["invalid timestamp", { now: NaN }],
    ["missing worker list", { workers: null as unknown as FleetWorkerObservation[] }],
    ["sparse worker list", { workers: new Array<FleetWorkerObservation>(1) }],
    ["sparse demand list", { demands: new Array<{ id: string; capability: string }>(1) }],
    ["sparse worker capabilities", { workers: [worker({ capabilities: new Array<string>(1) })] }],
  ])("rejects malformed snapshot: %s", (_label, overrides) => {
    const result = planFleet(policy, snapshot({ demands: demand, ...overrides }), prior);
    noActions(result);
    expect(result.reasons).toContain("invalid_snapshot");
    expect(() => JSON.stringify(result)).not.toThrow();
  });

  it.each([
    { lastObservedAt: NOW + 1, demandSamples: 1 },
    { lastObservedAt: NOW - 60_000, demandSamples: Infinity },
    { lastObservedAt: null, demandSamples: 2 },
    { lastObservedAt: NOW - 60_000, demandSamples: -1 },
  ])("rejects malformed sampling history %#", (history) => {
    const result = planFleet(policy, snapshot({ demands: demand }), history);
    noActions(result);
    expect(result.reasons).toContain("invalid_history");
  });

  it("returns a deterministic serializable plan without mutating input", () => {
    const input = snapshot({ workers: [worker({ id: "z" }), worker({ id: "a" })] });
    const before = JSON.stringify(input);
    const result = planFleet(policy, input);
    expect(planFleet(policy, input)).toEqual(result);
    expect(JSON.parse(JSON.stringify(result))).toEqual(result);
    expect(JSON.stringify(input)).toBe(before);
  });
});

describe("validateFleetPolicy", () => {
  it("accepts complete configuration and returns its policy", () => {
    expect(validateFleetPolicy(policy)).toEqual(policy);
  });
  it.each([null, {}, { ...policy, mode: "unsafe" }, { ...policy, maxWorkers: NaN },
    { ...policy, enabled: 1 }, { ...policy, scaleOutSamples: 1 },
    { ...policy, capabilities: [] }, { ...policy, capabilities: new Array<string>(1) }, { ...policy, pollIntervalSeconds: 1.5 },
    { ...policy, idleSeconds: Number.MAX_SAFE_INTEGER }])("rejects malformed configuration %#", (input) => {
    expect(() => validateFleetPolicy(input)).toThrow(Error);
  });
});
