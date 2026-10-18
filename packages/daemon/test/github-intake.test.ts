import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { GithubIntakeService, type GithubIntakeConfig, type GithubIssue, type GithubIntakeIO, type GithubTask } from "../src/domain/github-intake.js";

const config: GithubIntakeConfig = { enabled: true, assignee: "operator", repositories: [{ id: "R_1", name: "org/repo" }], targets: [{ host: "host-a", session: "worker@rig", physicalHost: "machine-a", capacity: 1, capabilities: ["code"] }], hostCapacity: 3 };
const issue = (number = 1): GithubIssue => ({ repositoryId: "R_1", repository: "org/repo", number, url: `https://github.com/org/repo/issues/${number}`, title: "Fix", body: "details", updatedAt: "2026-10-17T00:00:00Z", state: "open", assignees: ["operator"] });
const databases: ReturnType<typeof createDb>[] = [];
afterEach(() => { for (const db of databases.splice(0)) db.close(); });
function setup(issues = [issue()], databasePath?: string) {
  const db = createDb(databasePath); databases.push(db); migrate(db, ALL_MIGRATIONS);
  const queue = new QueueRepository(db, new EventBus(db));
  const tasks = new Map<string, GithubTask>();
  const io: GithubIntakeIO = {
    listIssues: vi.fn(async () => issues),
    observeTargets: vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0 }]),
    getTask: vi.fn(async (_host, id) => tasks.get(id) ?? null),
    createTask: vi.fn(async (_host, payload) => { const task = await queue.create(payload); tasks.set(task.qitemId, task); return task; }),
  };
  const service = new GithubIntakeService(db, queue, io); service.configure(config);
  const ready = (number = 1) => service.markEligibility({ repositoryId: "R_1", number, revision: issue().updatedAt, eligibility: "ready", capability: "code", reason: "ownership, branch and dependencies checked" });
  return { db, queue, tasks, io, service, ready };
}
describe("GitHub intake durable native queue dispatch", () => {
  it("deduplicates twenty overlapping polls and preserves reservation across restart", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    await Promise.all(Array.from({ length: 20 }, () => f.service.reconcile()));
    expect(f.io.createTask).toHaveBeenCalledTimes(1);
    const restarted = new GithubIntakeService(f.db, f.queue, f.io); await restarted.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(1);
    expect(restarted.status().issues[0]?.pickup).toBe("unclaimed");
  });
  it("reads back a timed-out create using its stable ID", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    const create = f.io.createTask;
    f.io.createTask = vi.fn(async (host, payload) => { await create(host, payload); throw new Error("timeout"); });
    await f.service.reconcile();
    const id = f.service.status().issues[0]?.qitemId;
    expect(id).toBeTruthy(); await f.service.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(1);
    expect(f.service.status().issues[0]?.qitemId).toBe(id);
  });
  it("retains an unknown reservation without rerouting or retry creation", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    f.io.createTask = vi.fn(async () => { throw new Error("timeout"); });
    await f.service.reconcile();
    f.io.getTask = vi.fn(async () => { throw new Error("host unreachable"); });
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
    expect(f.service.status().issues[0]?.error).toContain("unreachable");
  });
  it.each(["closed", "reassigned", "changed"])("blocks new dispatch when issue is %s", async (change) => {
    const issues = [issue()]; const f = setup(issues); await f.service.reconcile(); f.ready();
    if (change === "closed") issues[0]!.state = "closed";
    if (change === "reassigned") issues[0]!.assignees = [];
    if (change === "changed") issues[0]!.updatedAt = "new revision";
    await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
  });
  it("rejects out-of-scope snapshots and approvals", async () => {
    const f = setup([{ ...issue(), repositoryId: "foreign" }]);
    await f.service.reconcile(); expect(f.service.status().error).toContain("scope");
    expect(() => f.ready()).toThrow(); expect(f.io.createTask).not.toHaveBeenCalled();
  });
  it("reports Github and host failures without treating them as empty capacity", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    f.io.observeTargets = vi.fn(async () => { throw new Error("host unknown"); });
    await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
    f.io.listIssues = vi.fn(async () => { throw new Error("Github unavailable"); });
    await f.service.reconcile(); expect(f.service.status().issues).toHaveLength(1);
    expect(f.service.status().error).toContain("Github unavailable");
  });
  it("waiting for a human releases execution capacity while retaining issue custody", async () => {
    const f = setup([issue(), issue(2)]); await f.service.reconcile(); f.ready(); f.ready(2);
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
    const first = [...f.tasks.values()][0]!; first.state = "blocked"; first.waitingForHuman = true;
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(2);
    expect(f.service.status().issues[0]?.qitemId).toBe(first.qitemId);
  });
  it("never exceeds session or physical host capacity with eligible backlog", async () => {
    const f = setup([issue(), issue(2), issue(3)]); await f.service.reconcile(); f.ready(); f.ready(2); f.ready(3);
    await f.service.reconcile(); await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("retries a confirmed absent timeout reservation with exactly the same ID", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    f.io.createTask = vi.fn(async () => { throw new Error("timeout before write"); });
    await f.service.reconcile(); const id = f.service.status().issues[0]!.qitemId;
    const restarted = new GithubIntakeService(f.db, f.queue, f.io); await restarted.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(2);
    expect((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls.map(call => call[1].qitemId)).toEqual([id, id]);
  });
  it("uses durable triage and requires it to finish before readiness approval", async () => {
    const f = setup(); f.service.configure({ ...config, manager: { host: "host-a", session: "worker@rig" } });
    await f.service.reconcile(); await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
    expect(() => f.ready()).toThrow("Finish");
    [...f.tasks.values()][0]!.state = "done";
    await f.service.reconcile(); f.ready(); await f.service.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(2);
    expect(f.service.status().issues[0]?.stage).toBe("ready");
  });
  it.each([
    { active: 1, hostActive: 5, capacity: 2, available: true, expected: 1 },
    { active: 0, hostActive: 5, capacity: 2, available: true, expected: 0 },
    { active: 1, hostActive: null, capacity: 2, available: true, expected: 0 },
    { active: 1, hostActive: 5, capacity: 1, available: true, expected: 0 },
    { active: 1, hostActive: 5, capacity: 2, available: false, expected: 0 },
  ])("admits triage only on an already occupied known manager: %j", async ({ active, hostActive, capacity, available, expected }) => {
    const f = setup();
    f.service.configure({ ...config, manager: { host: "host-a", session: "worker@rig" }, targets: [{ ...config.targets[0]!, capacity }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", active, hostActive, available }]);
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(expected);
  });
  it("keeps ready work on an occupied manager subject to the physical limit", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    f.service.configure({ ...config, manager: { host: "host-a", session: "worker@rig" }, targets: [{ ...config.targets[0]!, capacity: 2 }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", active: 1, hostActive: 5, available: true }]);
    await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
  });
  it("retries absent triage on the still occupied current manager using the reserved ID", async () => {
    const f = setup();
    f.service.configure({ ...config, manager: { host: "host-a", session: "worker@rig" }, targets: [{ ...config.targets[0]!, capacity: 2 }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", active: 1, hostActive: 5, available: true }]);
    f.io.createTask = vi.fn(async () => { throw new Error("timeout before write"); });
    await f.service.reconcile(); const id = f.service.status().issues[0]!.qitemId;
    await f.service.reconcile();
    expect((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls.map(call => call[1].qitemId)).toEqual([id, id]);
  });
  it("does not treat a triage reservation as observed occupancy for retry", async () => {
    const f = setup();
    f.service.configure({ ...config, manager: { host: "host-a", session: "worker@rig" }, targets: [{ ...config.targets[0]!, capacity: 2 }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", active: 1, hostActive: 5, available: true }]);
    f.io.createTask = vi.fn(async () => { throw new Error("timeout before write"); });
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", active: 0, hostActive: 5, available: true }]);
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("counts independent external work and unknown reservations conservatively", async () => {
    const f = setup([issue(), issue(2)]); f.service.configure({ ...config, targets: [{ ...config.targets[0]!, capacity: 2 }] });
    await f.service.reconcile(); f.ready(); f.ready(2);
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 1 }]);
    f.io.createTask = vi.fn(async () => { throw new Error("timeout"); });
    await f.service.reconcile(); await f.service.reconcile();
    expect(new Set((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls.map(call => call[1].qitemId)).size).toBe(1);
  });
  it("caps multiple sessions on one physical host", async () => {
    const f = setup([issue(), issue(2)]);
    f.service.configure({ ...config, hostCapacity: 1, targets: [...config.targets, { ...config.targets[0]!, session: "other@rig" }] });
    f.io.observeTargets = vi.fn(async () => config.targets.concat([{ ...config.targets[0]!, session: "other@rig" }]).map(t => ({ host: t.host, session: t.session, active: 0, available: true })));
    await f.service.reconcile(); f.ready(); f.ready(2); await f.service.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("keeps a completed task as custody without closing or redispatching the issue", async () => {
    const f = setup(); await f.service.reconcile(); f.ready(); await f.service.reconcile();
    [...f.tasks.values()][0]!.state = "done"; await f.service.reconcile(); await f.service.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(1);
    expect(f.service.status().issues[0]?.issue.state).toBe("open");
    expect(() => f.ready()).toThrow("Finish");
  });

  it("reopens the on-disk database and retries the committed reservation", async () => {
    const dir = mkdtempSync(join(tmpdir(), "openrig-github-intake-"));
    try {
      const path = join(dir, "intake.db"); const f = setup([issue()], path);
      await f.service.reconcile(); f.ready();
      f.io.createTask = vi.fn(async () => { throw new Error("timeout before write"); });
      await f.service.reconcile(); const id = f.service.status().issues[0]!.qitemId;
      f.db.close(); databases.splice(databases.indexOf(f.db), 1);
      const db = createDb(path); databases.push(db);
      const queue = new QueueRepository(db, new EventBus(db));
      f.io.createTask = vi.fn(async (_host, payload) => queue.create(payload));
      const restarted = new GithubIntakeService(db, queue, f.io); await restarted.reconcile();
      expect(restarted.status().issues[0]?.qitemId).toBe(id);
      expect(queue.getById(id!)?.qitemId).toBe(id);
      db.close(); databases.splice(databases.indexOf(db), 1);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("keeps a confirmed absent reservation waiting when external work fills its slot", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    f.io.createTask = vi.fn(async () => { throw new Error("timeout before write"); });
    await f.service.reconcile(); const id = f.service.status().issues[0]!.qitemId;
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 1 }]);
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
    expect(f.service.status().issues[0]?.qitemId).toBe(id);
  });
  it("retriages a changed unowned approved revision with actionable CLI instructions", async () => {
    const issues = [issue()]; const f = setup(issues); await f.service.reconcile(); f.ready();
    f.service.configure({ ...config, manager: { host: "host-a", session: "worker@rig" } });
    issues[0]!.updatedAt = "changed"; await f.service.reconcile();
    expect(f.service.status().issues[0]?.eligibility).toBe("triage");
    expect(f.service.status().issues[0]?.stage).toBe("triage");
    const body = (f.io.createTask as ReturnType<typeof vi.fn>).mock.calls[0]![1].body;
    expect(body).toContain("rig github eligibility --file"); expect(body).toContain("rig github sync");
    expect(body).toContain('"repositoryId": "R_1"'); expect(body).toContain("rig queue update"); expect(body).toContain("--state done --closure-reason no-follow-on");
  });
  it.each(["disable", "approval"])("aborts stale dispatch after %s changes during host observation", async (change) => {
    const f = setup(); await f.service.reconcile(); f.ready();
    let release!: () => void; let entered!: () => void;
    const enteredPromise = new Promise<void>(resolve => { entered = resolve; });
    f.io.observeTargets = vi.fn(async () => { entered(); await new Promise<void>(resolve => { release = resolve; }); return [{ host: "host-a", session: "worker@rig", available: true, active: 0 }]; });
    const run = f.service.reconcile(); await enteredPromise;
    if (change === "disable") f.service.configure({ ...config, enabled: false });
    else f.service.markEligibility({ repositoryId: "R_1", number: 1, revision: issue().updatedAt, eligibility: "blocked", reason: "hold" });
    release(); await run; expect(f.io.createTask).not.toHaveBeenCalled(); expect(f.service.status().issues[0]?.qitemId).toBeNull();
  });

  it("holds a retry when another session fills the physical host", async () => {
    const f = setup();
    const other = { ...config.targets[0]!, session: "other@rig" };
    f.service.configure({ ...config, hostCapacity: 1, targets: [...config.targets, other] });
    await f.service.reconcile(); f.ready(); f.io.createTask = vi.fn(async () => { throw new Error("timeout"); });
    await f.service.reconcile();
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0 }, { host: "host-a", session: "other@rig", available: true, active: 1 }]);
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("preserves existing live custody with a source supersession error", async () => {
    const issues = [issue()]; const f = setup(issues); await f.service.reconcile(); f.ready(); await f.service.reconcile();
    const id = f.service.status().issues[0]!.qitemId;
    issues[0]!.updatedAt = "superseded"; await f.service.reconcile();
    expect(f.service.status().issues[0]?.qitemId).toBe(id); expect(f.service.status().issues[0]?.error).toContain("superseded");
    expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("does not publish a stale issue snapshot after configuration changes during GitHub IO", async () => {
    const f = setup(); let release!: (value: GithubIssue[]) => void; let entered!: () => void;
    const enteredPromise = new Promise<void>(resolve => { entered = resolve; });
    f.io.listIssues = vi.fn(async () => { entered(); return new Promise<GithubIssue[]>(resolve => { release = resolve; }); });
    const run = f.service.reconcile(); await enteredPromise; f.service.configure({ ...config, enabled: false }); release([issue()]); await run;
    expect(f.service.status().issues).toHaveLength(0); expect(f.io.createTask).not.toHaveBeenCalled();
  });

  it("dispatches manager critical priority before lower issue IDs", async () => {
    const f = setup([issue(), issue(2)]); await f.service.reconcile(); f.ready();
    f.service.markEligibility({ repositoryId: "R_1", number: 2, revision: issue().updatedAt, eligibility: "ready", capability: "code", reason: "Finish release review; owner confirmed and review capacity reserved", priority: "critical" });
    await f.service.reconcile();
    expect((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls[0]![1].body).toContain("/issues/2");
    expect((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls[0]![1].priority).toBe("critical");
  });
  it("delivers the reviewed downstream action without instructing duplicate implementation", async () => {
    const f = setup(); f.service.configure({ ...config, targets: [{ ...config.targets[0]!, capabilities: ["review"] }] });
    await f.service.reconcile();
    f.service.markEligibility({ repositoryId: "R_1", number: 1, revision: issue().updatedAt, eligibility: "ready", capability: "review", reason: "Review existing PR; preserve its writer; acceptance reviewer available", priority: "urgent" });
    await f.service.reconcile(); const payload = (f.io.createTask as ReturnType<typeof vi.fn>).mock.calls[0]![1];
    expect(payload.body).toContain("Carry out the manager-reviewed next action");
    expect(payload.body).toContain("Review existing PR; preserve its writer; acceptance reviewer available");
    expect(payload.body).toContain("Capability: review"); expect(payload.body).not.toContain("Implement the reviewed");
  });

  it("uses durable intake age before issue ID within equal manager priority", async () => {
    const issues = [issue(2)]; const f = setup(issues); let now = new Date("2026-10-17T00:00:00Z");
    const controlled = new GithubIntakeService(f.db, f.queue, f.io, () => now);
    await controlled.reconcile(); now = new Date("2026-10-17T01:00:00Z"); issues.push(issue());
    await controlled.reconcile(); f.ready(); f.ready(2); await controlled.reconcile();
    expect((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls[0]![1].body).toContain("/issues/2");
    expect((f.io.createTask as ReturnType<typeof vi.fn>).mock.calls[0]![1].priority).toBe("routine");
  });

  it.each(["handoff", "handoffAndComplete"] as const)("tracks real native %s successor custody", async (method) => {
    const f = setup(); await f.service.reconcile(); f.ready(); await f.service.reconcile();
    const root = f.service.status().issues[0]!.qitemId!;
    const handed = await f.queue[method]({ qitemId: root, fromSession: "worker@rig", toSession: "reviewer@rig", nudge: false });
    expect(handed.closed.handedOffTo).toBe("reviewer@rig");
    f.io.getTask = vi.fn(async (_host, id) => f.queue.getById(id));
    f.io.findSuccessor = vi.fn(async (host, task) => ({ host, task: f.queue.getById(handed.created.qitemId)! }));
    await f.service.reconcile();
    expect(f.service.status().issues[0]?.pickup).toBe("unclaimed");
    expect(f.service.status().issues[0]?.currentSession).toBe("reviewer@rig");
    expect(f.service.status().issues[0]?.error).toBeNull();
  });
  it("follows real native cross-host completion closureTarget without treating session as ID", async () => {
    const f = setup(); await f.service.reconcile(); f.ready(); await f.service.reconcile();
    const root = f.service.status().issues[0]!.qitemId!;
    const successor = await f.queue.create({ qitemId: "qitem-native-cross-successor", sourceSession: "worker@rig", destinationSession: "reviewer@rig", body: "Review", chainOfRecord: [root] });
    f.queue.closeCrossHostHandoffSource({ qitemId: root, fromSession: "worker@rig", toSession: "reviewer@rig", closureTarget: `${successor.qitemId}@host-b`, terminalState: "done" });
    f.io.getTask = vi.fn(async (_host, id) => f.queue.getById(id)); await f.service.reconcile();
    expect(f.io.getTask).toHaveBeenCalledWith("host-b", successor.qitemId);
    expect(f.service.status().issues[0]?.currentHost).toBe("host-b");
    expect(f.service.status().issues[0]?.pickup).toBe("unclaimed");
  });
  it("rejects duplicate repository names regardless of spelling or ID representation", () => {
    const f = setup(); expect(() => f.service.configure({ ...config, repositories: [{ id: "R_1", name: "org/repo" }, { id: "123", name: "ORG/REPO" }] })).toThrow();
  });
  it("canonicalizes a changed repository ID representation while preserving native custody", async () => {
    const issues = [issue()]; const f = setup(issues); await f.service.reconcile(); f.ready(); await f.service.reconcile();
    const id = f.service.status().issues[0]!.qitemId;
    issues[0] = { ...issue(), repositoryId: "123", repositoryAliases: ["123", "R_1"] };
    f.service.configure({ ...config, repositories: [{ id: "123", name: "org/repo" }] });
    await f.service.reconcile();
    expect(f.service.status().issues).toHaveLength(1); expect(f.service.status().issues[0]?.qitemId).toBe(id);
    expect(f.service.status().issues[0]?.issue.repositoryId).toBe("123"); expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("blocks dispatch on unknown physical host occupancy including unconfigured work", async () => {
    const f = setup(); await f.service.reconcile(); f.ready();
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0, hostActive: null }]);
    await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0, hostActive: 3 }]);
    await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
  });

  it("accepts canonical numeric source identity while configuration uses verified node ID", async () => {
    const f = setup([{ ...issue(), repositoryId: "123", repositoryAliases: ["123", "R_1"] }]);
    await f.service.reconcile(); expect(f.service.status().error).toBeNull();
    expect(f.service.status().issues[0]?.issue.repositoryId).toBe("123");
    expect(f.service.status().issues[0]?.inScope).toBe(true);
  });
  it("retains unknown custody when a real native handoff successor cannot be read", async () => {
    const f = setup(); await f.service.reconcile(); f.ready(); await f.service.reconcile(); const root = f.service.status().issues[0]!.qitemId!;
    await f.queue.handoffAndComplete({ qitemId: root, fromSession: "worker@rig", toSession: "reviewer@rig", nudge: false });
    f.io.getTask = vi.fn(async (_host, id) => f.queue.getById(id)); f.io.findSuccessor = vi.fn(async () => null);
    await f.service.reconcile(); expect(f.service.status().issues[0]?.pickup).toBe("unknown");
    expect(f.service.status().issues[0]?.qitemId).toBe(root); expect(f.io.createTask).toHaveBeenCalledTimes(1);
  });
  it("rejects mapping the same transport to different physical machines", () => {
    const f = setup(); expect(() => f.service.configure({ ...config, targets: [...config.targets, { ...config.targets[0]!, session: "other@rig", physicalHost: "false-machine" }] })).toThrow();
  });

  it("batches onto an already occupied seat at full physical capacity without opening another seat", async () => {
    const f = setup([issue(), issue(2), issue(3)]);
    f.service.configure({ ...config, hostCapacity: 1, targets: [{ ...config.targets[0]!, capacity: 3 }, { ...config.targets[0]!, session: "empty@rig" }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 1, hostActive: 1 }, { host: "host-a", session: "empty@rig", available: true, active: 0, hostActive: 1 }]);
    await f.service.reconcile(); f.ready(); f.ready(2); f.ready(3); await f.service.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(2);
    expect([...f.tasks.values()].map(t => t.destinationSession)).toEqual(["worker@rig", "worker@rig"]);
  });
  it("denies a new empty seat when all three physical seats are occupied", async () => {
    const f = setup(); f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0, hostActive: 3 }]);
    await f.service.reconcile(); f.ready(); await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
  });
  it("counts multiple uncertain reservations on one seat once physically and individually per session", async () => {
    const f = setup([issue(), issue(2), issue(3)]); f.service.configure({ ...config, hostCapacity: 1, targets: [{ ...config.targets[0]!, capacity: 2 }] });
    await f.service.reconcile(); f.ready(); f.ready(2); f.ready(3);
    f.io.createTask = vi.fn(async () => { throw new Error("uncertain"); }); await f.service.reconcile();
    expect(f.io.createTask).toHaveBeenCalledTimes(2);
    expect(f.service.status().issues.filter(row => row.qitemId)).toHaveLength(2);
    await f.service.reconcile(); expect(f.service.status().issues.filter(row => row.qitemId)).toHaveLength(2);
  });
  it("uses occupied-seat fallback when hostActive is omitted even with ten packets", async () => {
    const f = setup(); f.service.configure({ ...config, hostCapacity: 2, targets: [{ ...config.targets[0]!, capacity: 10, capabilities: [] }, { ...config.targets[0]!, session: "second@rig" }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 10 }, { host: "host-a", session: "second@rig", available: true, active: 0 }]);
    await f.service.reconcile(); f.ready(); await f.service.reconcile();
    expect(f.service.status().issues[0]?.session).toBe("second@rig");
  });
  it("retries held reservations at a full host without charging their seat twice", async () => {
    const f = setup([issue(), issue(2)]); f.service.configure({ ...config, hostCapacity: 1, targets: [{ ...config.targets[0]!, capacity: 2 }] });
    await f.service.reconcile(); f.ready(); f.ready(2); f.io.createTask = vi.fn(async () => { throw new Error("timeout before write"); });
    await f.service.reconcile(); const ids = f.service.status().issues.map(row => row.qitemId); expect(ids.every(Boolean)).toBe(true);
    await f.service.reconcile(); expect(f.io.createTask).toHaveBeenCalledTimes(4); expect(f.service.status().issues.map(row => row.qitemId)).toEqual(ids);
  });

  it("aggregates distinct transport host totals conservatively on the same physical machine", async () => {
    const f = setup(); f.service.configure({ ...config, hostCapacity: 2, targets: [...config.targets, { ...config.targets[0]!, host: "host-alias", capabilities: [] }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0, hostActive: 1 }, { host: "host-alias", session: "worker@rig", available: true, active: 0, hostActive: 1 }]);
    await f.service.reconcile(); f.ready(); await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
  });

  it("denies dispatch when a configured transport on the physical machine has no observation", async () => {
    const f = setup(); f.service.configure({ ...config, targets: [...config.targets, { ...config.targets[0]!, host: "missing-host", capabilities: [] }] });
    f.io.observeTargets = vi.fn(async () => [{ host: "host-a", session: "worker@rig", available: true, active: 0, hostActive: 0 }]);
    await f.service.reconcile(); f.ready(); await f.service.reconcile(); expect(f.io.createTask).not.toHaveBeenCalled();
  });

});

it('atomically preserves the first reservation when independent SQLite consumers resume stale source snapshots', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'intake-source-race-'));
  try {
    const file = join(dir, 'db.sqlite'); const f = setup([issue(), issue(2)], file);
    await f.service.reconcile(); f.ready(); f.ready(2); await f.service.reconcile();
    const first = [...f.tasks.values()][0]!; first.state = 'done';
    const otherDb = createDb(file); databases.push(otherDb);
    const second = new GithubIntakeService(otherDb, new QueueRepository(otherDb, new EventBus(otherDb)), f.io);
    let readers = 0; let release!: () => void; let both!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; }); const arrived = new Promise<void>(resolve => { both = resolve; });
    f.io.getTask = async () => { if (++readers === 2) both(); await barrier; return first; };
    const before = vi.mocked(f.io.createTask).mock.calls.length;
    const running = [f.service.reconcile(), second.reconcile()]; await arrived; release(); await Promise.all(running);
    expect(vi.mocked(f.io.createTask).mock.calls.length - before).toBe(1);
    const assigned = f.service.status().issues.find(row => row.issue.number === 2)!;
    expect(assigned.qitemId).toBeTruthy(); expect(second.status().issues.find(row => row.issue.number === 2)!.qitemId).toBe(assigned.qitemId);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
