import { describe, expect, it, vi, afterEach } from "vitest";
import { Command } from "commander";
import { DaemonClient } from "../src/client.js";
import { githubCommand } from "../src/commands/github.js";
import { STATE_FILE, type LifecycleDeps } from "../src/daemon-lifecycle.js";
import { createProgram } from "../src/index.js";

describe("GitHub intake CLI", () => {
  afterEach(() => { vi.unstubAllEnvs(); vi.restoreAllMocks(); });
  it("exposes configuration, reconciliation, eligibility and status on the native CLI", () => {
    const command = createProgram().commands.find((entry) => entry.name() === "github");
    expect(command, "GitHub intake must be operable without a browser").toBeDefined();
    expect(command!.commands.map((entry) => entry.name()).sort()).toEqual(["configure", "eligibility", "status", "sync"]);
  });
  it.each(["status", "sync"])("authenticates %s with the configured operator environment token", async verb => {
    vi.stubEnv("OPENRIG_AUTH_BEARER_TOKEN", "test-operator-token");
    vi.spyOn(console, "log").mockImplementation(() => {});
    const fetchImpl = vi.fn(async (_input: string | URL | Request, _init?: RequestInit) => new Response(JSON.stringify({ config: null }), { status: 200 }));
    const client = new DaemonClient("http://127.0.0.1:7433", { fetchImpl });
    const lifecycleDeps = {
      exists: (p: string) => p === STATE_FILE,
      readFile: () => JSON.stringify({ pid: 123, port: 7433, db: "test.sqlite", startedAt: "2026-04-20T00:00:00Z" }),
      isProcessAlive: () => true,
      fetch: async () => new Response(JSON.stringify({ status: "ok" }), { status: 200 }),
    } as LifecycleDeps;
    const program = new Command().addCommand(githubCommand({ lifecycleDeps, clientFactory: () => client }));
    await program.parseAsync(["github", verb, "--json"], { from: "user" });
    expect(fetchImpl).toHaveBeenCalledOnce();
    const init = fetchImpl.mock.calls[0]?.[1] as RequestInit | undefined;
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer test-operator-token");
  });
});
