import { afterEach, describe, expect, it } from "vitest";
import { createFullTestDb, createTestApp } from "./helpers/test-app.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { GithubIntakeService, type GithubIntakeIO } from "../src/domain/github-intake.js";
const databases: ReturnType<typeof createFullTestDb>[] = [];
afterEach(() => { for (const db of databases.splice(0)) db.close(); });
const io: GithubIntakeIO = { listIssues: async () => [], observeTargets: async () => [], getTask: async () => null, createTask: async () => { throw new Error("No configured target"); } };
const config = { enabled: false, assignee: "operator", repositories: [{ id: "42", name: "owner/project" }], targets: [], hostCapacity: 3 };
function setup(token: string | null = null) {
  const db = createFullTestDb(); databases.push(db);
  const service = new GithubIntakeService(db, new QueueRepository(db, new EventBus(db)), io);
  return { service, app: createTestApp(db, { appDeps: { githubIntakeService: service, missionControlBearerToken: token } }).app };
}
describe("GitHub intake API registration", () => {
  it("persists explicit configuration and reports actual reconciliation state", async () => {
    const { app } = setup();
    const configured = await app.request("/api/github-intake/configure", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(config) });
    expect(configured.status).toBe(200);
    const status = await app.request("/api/github-intake");
    expect((await status.json()).config).toEqual(config);
    expect((await app.request("/api/github-intake/reconcile", { method: "POST" })).status).toBe(200);
  });
  it("protects dispatch configuration with the existing operator bearer", async () => {
    const { app, service } = setup("operator-test-token");
    const response = await app.request("/api/github-intake/configure", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(config) });
    expect(response.status).toBe(401);
    expect(service.status().config).toBeNull();
  });
  it("rejects invalid configuration without writing it", async () => {
    const { app, service } = setup();
    const response = await app.request("/api/github-intake/configure", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...config, hostCapacity: -1 }) });
    expect(response.status).toBe(400);
    expect(service.status().config).toBeNull();
  });
});
