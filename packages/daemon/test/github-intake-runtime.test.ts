import { afterEach, describe, expect, it, vi } from "vitest";
import { createGithubIntakeIO, readGithubIssues } from "../src/adapters/github-intake.js";
import { GithubIntakeService, type GithubIntakeConfig } from "../src/domain/github-intake.js";

const config: GithubIntakeConfig = { enabled: true, assignee: "operator", repositories: [{ id: "42", name: "owner/project" }], targets: [], hostCapacity: 3 };
const issue = (number: number) => ({ number, html_url: `https://github.com/owner/project/issues/${number}`, title: "Fix the issue", body: "Details", updated_at: "2026-10-17T00:00:00Z", state: "open", assignees: [{ login: "operator" }] });

describe("GitHub source boundary", () => {
  it("reads all pages and excludes pull requests without shell interpolation", async () => {
    const run = vi.fn(async (_command: string, args: string[]) => args.includes("--paginate")
      ? JSON.stringify([[issue(1), { ...issue(2), pull_request: {} }], [issue(3)]])
      : JSON.stringify({ id: 42 }));
    const issues = await readGithubIssues(config, run);
    expect(issues.map(i => i.number)).toEqual([1, 3]);
    expect(run.mock.calls[1][1]).toContain("--slurp");
    expect(run.mock.calls[1][1]).toContain("repos/owner/project/issues?state=open&assignee=operator&per_page=100");
  });
  it("refuses a node-id match without a canonical numeric repository identity", async () => {
    await expect(readGithubIssues({ ...config, repositories: [{ id: "R_node42", name: "owner/project" }] }, async () => '{"node_id":"R_node42"}')).rejects.toThrow(/identity/);
  });
  it("refuses repository identity drift", async () => {
    await expect(readGithubIssues(config, async () => JSON.stringify({ id: 43 }))).rejects.toThrow(/identity/);
  });
  it("does not report an incomplete or failed page stream as an empty backlog", async () => {
    let count = 0;
    await expect(readGithubIssues(config, async () => ++count === 1 ? '{"id":42}' : '{"message":"rate limited"}')).rejects.toThrow(/page/);
    await expect(readGithubIssues(config, async () => { throw new Error("GitHub unavailable"); })).rejects.toThrow("GitHub unavailable");
  });
  it("refuses incomplete issue records", async () => {
    let count = 0;
    await expect(readGithubIssues(config, async () => ++count === 1 ? '{"id":42}' : '[[{"number":1}]]')).rejects.toThrow(/issue/);
  });
});

import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFullTestDb, createTestApp } from "./helpers/test-app.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import type { HttpHostEntry } from "../src/domain/hosts/hosts-registry-reader.js";
import type { RemoteJsonResult } from "../src/domain/hosts/remote-daemon-http.js";
const cleanup: (() => void)[] = [];
afterEach(() => { for (const close of cleanup.splice(0)) close(); });
function runtime(reply: (path: string) => RemoteJsonResult) {
  const db = createFullTestDb(); cleanup.push(() => db.close());
  const directory = mkdtempSync(join(tmpdir(), "github-intake-")); cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
  const hostsPath = join(directory, "hosts.yaml");
  writeFileSync(hostsPath, "hosts:\n  - id: worker\n    transport: http\n    url: http://127.0.0.1:12345\n");
  const request = vi.fn(async (_host: HttpHostEntry, path: string) => reply(path));
  return { io: createGithubIntakeIO({ queue: new QueueRepository(db, new EventBus(db)), localNodes: () => [], hostsPath, request }), request };
}
const targetConfig: GithubIntakeConfig = { ...config, targets: [{ host: "worker", physicalHost: "vm", session: "coder@project", capacity: 1, capabilities: ["code"] }] };
const success = (payload: unknown): RemoteJsonResult => ({ ok: true, status: 200, payload });
const node = (session: string, busy: boolean) => ({ canonicalSessionName: session, sessionStatus: "running", startupStatus: "ready", terminalActive: busy, agentActivity: { state: busy ? "running" : "idle" } });
describe("GitHub native fleet transport", () => {
  it("counts ten pending packets on one busy manager as one occupied physical seat", async () => {
    const manager = "manager@project";
    const { io } = runtime(path => path.startsWith("/api/ps") ? success([{ rigId: "live" }])
      : path.includes("/nodes") ? success([node(manager, true), node("coder@project", false)])
        : success(Array.from({ length: 10 }, (_, i) => ({ qitemId: `qitem-manager-${i}`, destinationSession: manager }))));
    const targets = [targetConfig.targets[0]!, { ...targetConfig.targets[0]!, session: manager, capacity: 20 }];
    expect(await io.observeTargets({ ...targetConfig, targets })).toEqual([
      { host: "worker", session: "coder@project", available: true, active: 0, hostActive: 1 },
      { host: "worker", session: manager, available: true, active: 10, hostActive: 1 },
    ]);
  });
  it("dispatches a second native worker beside ten real manager packets under a physical cap of three", async () => {
    const db = createFullTestDb(); cleanup.push(() => db.close());
    const queue = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
    for (let i = 0; i < 10; i++) await queue.create({ sourceSession: "intake@system", destinationSession: "manager@project", body: `Triage ${i}`, nudge: false });
    const io = createGithubIntakeIO({ queue, localNodes: () => [node("manager@project", true), node("coder@project", false)],
      runGithub: async (_command, args) => JSON.stringify(args.includes("--paginate") ? [[issue(1)]] : { id: 42 }) });
    const service = new GithubIntakeService(db, queue, io);
    service.configure({ ...config, targets: [{ host: "local", physicalHost: "vm", session: "manager@project", capacity: 20, capabilities: ["triage"] }, { host: "local", physicalHost: "vm", session: "coder@project", capacity: 1, capabilities: ["code"] }] });
    await service.reconcile(); service.markEligibility({ repositoryId: "42", number: 1, revision: issue(1).updated_at, eligibility: "ready", capability: "code", reason: "Existing ownership and downstream capacity checked" });
    await service.reconcile();
    expect(service.status().issues[0]?.session).toBe("coder@project");
    expect(queue.list({ destinationSession: "manager@project", limit: 20 })).toHaveLength(10);
    expect(queue.list({ destinationSession: "coder@project", limit: 20 })).toHaveLength(1);
  });

  it("counts unconfigured writers and archived running seats before dispatch", async () => {
    const { io } = runtime(path => {
      if (path.startsWith("/api/ps")) return success([{ rigId: "live", archivedAt: null }, { rigId: "old", archivedAt: "yesterday" }]);
      if (path.includes("/live/nodes")) return success([node("coder@project", false), node("other@project", true)]);
      if (path.includes("/old/nodes")) return success([node("old@project", true)]);
      return success([{ destinationSession: "other@project" }]);
    });
    expect(await io.observeTargets(targetConfig)).toEqual([{ host: "worker", session: "coder@project", available: true, active: 0, hostActive: 2 }]);
  });
  it("does not count unanswered human requests as machine execution", async () => {
    const { io } = runtime(path => path.startsWith("/api/ps") ? success([{ rigId: "live" }])
      : path.includes("/nodes") ? success([node("coder@project", false)])
        : success(["human@kernel", "operator@external", "human-operator@host"].map(destinationSession => ({ destinationSession }))));
    expect(await io.observeTargets(targetConfig)).toMatchObject([{ available: true, active: 0, hostActive: 0 }]);
  });
  it("holds capacity unknown when inventory is unavailable or truncated", async () => {
    for (const failure of [false, true]) {
      const { io } = runtime(path => path.startsWith("/api/ps") ? success([{ rigId: "live" }])
        : path.includes("/nodes") ? success([node("coder@project", false)])
        : failure ? success(Array.from({ length: 200 }, () => ({ destinationSession: "other@project" })))
          : { ok: false, kind: "timeout", detail: "unknown" });
      expect(await io.observeTargets(targetConfig)).toEqual([{ host: "worker", session: "coder@project", available: false, active: 0, hostActive: null }]);
    }
  });
  it("distinguishes a confirmed absent queue item from uncertain delivery", async () => {
    const absent = runtime(() => ({ ok: false, kind: "http", status: 404, detail: "missing" }));
    expect(await absent.io.getTask("worker", "qitem-test")).toBeNull();
    const unknown = runtime(() => ({ ok: false, kind: "network", detail: "lost" }));
    await expect(unknown.io.getTask("worker", "qitem-test")).rejects.toThrow(/unavailable/);
    await expect(unknown.io.createTask("worker", { qitemId: "qitem-test", sourceSession: "manager@project", destinationSession: "coder@project", body: "Work" })).rejects.toThrow(/uncertain/);
    expect(unknown.request.mock.calls[0][0].id).toBe("worker");
  });
  it("delivers and reads back the same stable ID through the real native queue HTTP routes", async () => {
    const db = createFullTestDb(); cleanup.push(() => db.close());
    const queue = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
    const app = createTestApp(db, { appDeps: { queueRepo: queue } }).app;
    const directory = mkdtempSync(join(tmpdir(), "github-native-http-")); cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const hostsPath = join(directory, "hosts.yaml");
    writeFileSync(hostsPath, "hosts:\n  - id: worker\n    transport: http\n    url: http://127.0.0.1:12345\n");
    const io = createGithubIntakeIO({ queue, localNodes: () => [], hostsPath, request: async (_host, path, options) => {
      const response = await app.request(path, { method: options.method, headers: { ...options.headers, "Content-Type": "application/json" }, ...(options.body ? { body: JSON.stringify(options.body) } : {}) });
      return response.ok ? { ok: true, status: response.status, payload: await response.json() }
        : { ok: false, kind: "http", status: response.status, detail: await response.text() };
    } });
    const input = { qitemId: "qitem-http-stable", sourceSession: "github-intake", destinationSession: "coder@project", body: "Review the existing PR", nudge: false };
    expect(await io.createTask("worker", input)).toMatchObject({ qitemId: input.qitemId, state: "pending" });
    expect(await io.createTask("worker", input)).toMatchObject({ qitemId: input.qitemId });
    expect(await io.getTask("worker", input.qitemId)).toMatchObject({ qitemId: input.qitemId, destinationSession: "coder@project" });
    expect(queue.list()).toHaveLength(1);
  });
  it("resolves native session-addressed handoffs through exact successor lineage", async () => {
    const { io, request } = runtime(() => success([{ qitemId: "qitem-next", state: "pending", destinationSession: "reviewer@project", handedOffFrom: "qitem-first" }]));
    expect(await io.findSuccessor!("worker", { qitemId: "qitem-first", state: "done", destinationSession: "coder@project", handedOffTo: "reviewer@project" })).toMatchObject({ host: "worker", task: { qitemId: "qitem-next" } });
    expect(request.mock.calls[0][1]).toBe("/api/queue/list?handedOffFrom=qitem-first&limit=2");
  });
  it("filters native successor queries before the result bound", async () => {
    const db = createFullTestDb(); cleanup.push(() => db.close());
    const queue = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
    const root = await queue.create({ sourceSession: "manager@project", destinationSession: "coder@project", body: "work" });
    const handoff = await queue.handoff({ qitemId: root.qitemId, fromSession: "coder@project", toSession: "reviewer@project", nudge: false });
    await queue.create({ sourceSession: "manager@project", destinationSession: "reviewer@project", body: "unrelated" });
    const io = createGithubIntakeIO({ queue, localNodes: () => [] });
    expect(await io.findSuccessor!("local", { ...root, handedOffTo: "reviewer@project" })).toMatchObject({ host: "local", task: { qitemId: handoff.created.qitemId } });
    expect(queue.list({ handedOffFrom: "absent", limit: 2 })).toEqual([]);
  });
  it("canonicalizes numeric and node-id aliases to the same source identity", async () => {
    const run = async (_command: string, args: string[]) => JSON.stringify(args.includes("--paginate") ? [[issue(1)]] : { id: 42, node_id: "R_node42" });
    const numeric = await readGithubIssues(config, run);
    const alias = await readGithubIssues({ ...config, repositories: [{ id: "R_node42", name: "owner/project" }] }, run);
    expect(alias).toEqual(numeric);
    expect(alias[0].repositoryId).toBe("42");
  });
});
