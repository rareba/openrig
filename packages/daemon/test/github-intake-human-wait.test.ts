import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFullTestDb } from "./helpers/test-app.js";
import { createGithubIntakeIO } from "../src/adapters/github-intake.js";
import { GithubIntakeService, type GithubIntakeConfig } from "../src/domain/github-intake.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import type { RemoteJsonResult } from "../src/domain/hosts/remote-daemon-http.js";

const cleanups: (() => void)[] = [];
afterEach(() => { for (const cleanup of cleanups.splice(0)) cleanup(); });
const success = (payload: unknown): RemoteJsonResult => ({ ok: true, status: 200, payload });
const human = { qitemId: "qitem-human", state: "pending", destinationSession: "human@kernel", blockedOn: null, humanIntent: "decision" };
const worker = { qitemId: "qitem-worker", state: "blocked", destinationSession: "coder@project", blockedOn: human.qitemId };
function setup(reply?: (path: string) => RemoteJsonResult) {
  const db = createFullTestDb(); cleanups.push(() => db.close());
  const queue = new QueueRepository(db, new EventBus(db), { validateRig: () => true });
  queue.attachOutbox(new OutboxHandler(db));
  const dir = mkdtempSync(join(tmpdir(), "github-human-wait-")); cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
  const hostsPath = join(dir, "hosts.yaml");
  writeFileSync(hostsPath, "hosts:\n  - id: worker\n    transport: http\n    url: http://worker:7433\n");
  const request = vi.fn(async (_host, path: string) => reply ? reply(path) : success(null));
  const io = createGithubIntakeIO({ queue, hostsPath, request, localNodes: () => [{ canonicalSessionName: "coder@project", sessionStatus: "running", startupStatus: "ready", terminalActive: false, agentActivity: { state: "idle" } }],
    runGithub: async (_command, args) => JSON.stringify(args.includes("--paginate") ? [[1, 2].map(number => ({ number, html_url: `https://github.com/owner/project/issues/${number}`, title: "Fix", body: "Details", updated_at: "2026-10-17T00:00:00Z", state: "open", assignees: [{ login: "operator" }] }))] : { id: 42 }) });
  return { db, queue, io, request, hostsPath };
}

describe("native qitem human-wait projection", () => {
  it("releases execution capacity for a worker parked on a real native human decision", async () => {
    const { db, queue, io, hostsPath } = setup();
    const config: GithubIntakeConfig = { enabled: true, assignee: "operator", repositories: [{ id: "42", name: "owner/project" }], targets: [{ host: "local", physicalHost: "machine", session: "coder@project", capacity: 1, capabilities: ["code"] }], hostCapacity: 1 };
    const service = new GithubIntakeService(db, queue, io); service.configure(config);
    await service.reconcile();
    for (const number of [1, 2]) service.markEligibility({ repositoryId: "42", number, revision: "2026-10-17T00:00:00Z", eligibility: "ready", capability: "code", reason: "Owner and prerequisites verified." });
    await service.reconcile();
    const first = service.status().issues.find(row => row.issue.number === 1)!;
    const decision = await queue.create({ sourceSession: "coder@project", destinationSession: "human@kernel", body: "Release authority needed", summary: "Release decision", evidenceRef: "https://example.com/evidence", humanIntent: "decision", nudge: false });
    queue.claim({ qitemId: first.qitemId!, destinationSession: "coder@project" });
    queue.update({ qitemId: first.qitemId!, actorSession: "coder@project", state: "blocked", blockedOn: decision.qitemId, transitionNote: "Waiting for release authority." });
    const localGet = io.getTask;
    const uncertainRemote = createGithubIntakeIO({ queue, hostsPath, localNodes: () => [], request: async (_host, path) =>
      path.endsWith(first.qitemId!) ? success(queue.getById(first.qitemId!)) : { ok: false, kind: "timeout", detail: "Human blocker unavailable" } });
    io.getTask = (_host, id) => uncertainRemote.getTask("worker", id);
    await service.reconcile();
    expect(service.status().issues.find(row => row.issue.number === 1)?.pickup).toBe("unknown");
    expect(service.status().issues.find(row => row.issue.number === 2)?.qitemId).toBeNull();
    io.getTask = localGet;
    expect(await io.getTask("local", first.qitemId!)).toMatchObject({ waitingForHuman: true });
    await service.reconcile();
    expect(service.status().issues.find(row => row.issue.number === 1)).toMatchObject({ qitemId: first.qitemId, pickup: "waiting-human" });
    expect(service.status().issues.find(row => row.issue.number === 2)?.qitemId).toBeTruthy();
    expect(queue.getById(first.qitemId!)?.state).toBe("blocked");
  });

  it.each([
    { label: "live decision", blocker: human, expected: true },
    { label: "legacy decision", blocker: { ...human, humanIntent: null }, expected: true },
    { label: "human update", blocker: { ...human, humanIntent: "update" }, expected: false },
    { label: "completed decision", blocker: { ...human, state: "done" }, expected: false },
    { label: "canceled decision", blocker: { ...human, state: "canceled" }, expected: false },
    { label: "ordinary agent", blocker: { ...human, destinationSession: "other@project", humanIntent: null }, expected: false },
  ])("projects $label with one same-host raw blocker read in getTask and findSuccessor", async ({ blocker, expected }) => {
    const { io, request } = setup(path => path.includes("/list?") ? success([{ ...worker, handedOffFrom: "qitem-root" }])
      : path.endsWith("qitem-worker") ? success(worker) : success(blocker));
    expect(await io.getTask("worker", worker.qitemId)).toMatchObject({ waitingForHuman: expected });
    expect(request.mock.calls.map(call => call[1])).toEqual(["/api/queue/qitem-worker", "/api/queue/qitem-human"]);
    request.mockClear();
    expect(await io.findSuccessor!("worker", { qitemId: "qitem-root", state: "done", destinationSession: "first@project", handedOffTo: "coder@project" })).toMatchObject({ task: { waitingForHuman: expected } });
    expect(request.mock.calls.map(call => call[1])).toEqual(["/api/queue/list?handedOffFrom=qitem-root&limit=2", "/api/queue/qitem-human"]);
  });

  it("excludes direct human updates and treats a confirmed absent blocker as non-human wait", async () => {
    const update = setup(() => success({ ...human, humanIntent: "update" }));
    expect(await update.io.getTask("worker", human.qitemId)).toMatchObject({ waitingForHuman: false, humanIntent: "update" });
    expect(update.request).toHaveBeenCalledTimes(1);
    const absent = setup(path => path.endsWith("qitem-worker") ? success(worker) : { ok: false, kind: "http", status: 404, detail: "Absent blocker" });
    expect(await absent.io.getTask("worker", worker.qitemId)).toMatchObject({ waitingForHuman: false });
    expect(absent.request).toHaveBeenCalledTimes(2);
  });

  it("does not recurse through a second ordinary qitem blocker", async () => {
    const { io, request } = setup(path => path.endsWith("qitem-worker") ? success(worker) : success({ ...human, destinationSession: "other@project", state: "blocked", humanIntent: null, blockedOn: "qitem-third" }));
    expect(await io.getTask("worker", worker.qitemId)).toMatchObject({ waitingForHuman: false });
    expect(request).toHaveBeenCalledTimes(2);
  });

  it.each(["unavailable", "malformed", "wrong-identity"])("keeps a remote %s blocker unknown instead of inventing free capacity", async mode => {
    const { io, request } = setup(path => path.endsWith("qitem-worker") ? success(worker)
      : mode === "unavailable" ? { ok: false, kind: "timeout", detail: "Unknown blocker" }
        : mode === "malformed" ? success({ qitemId: human.qitemId }) : success({ ...human, qitemId: "qitem-stranger" }));
    await expect(io.getTask("worker", worker.qitemId)).rejects.toThrow();
    expect(request).toHaveBeenCalledTimes(2);
  });
});
