import { readFileSync } from "node:fs";
import { Command } from "commander";
import { DaemonClient } from "../client.js";
import { daemonStatusGuard, getDaemonStatus, getDaemonUrl } from "../daemon-lifecycle.js";
import { realDeps } from "./daemon.js";
import type { StatusDeps } from "./status.js";

export function githubCommand(override?: StatusDeps): Command {
  const command = new Command("github").description("Durable GitHub issue intake and fleet dispatch");
  async function request(method: "GET" | "POST", path: string, body: unknown, json?: boolean) {
    const deps = override ?? { lifecycleDeps: realDeps(), clientFactory: (url: string) => new DaemonClient(url) };
    const status = await getDaemonStatus(deps.lifecycleDeps);
    if (!daemonStatusGuard(status, { json })) return;
    const client = deps.clientFactory(getDaemonUrl(status));
    const bearer = process.env.OPENRIG_AUTH_BEARER_TOKEN;
    const auth = bearer ? { headers: { Authorization: `Bearer ${bearer}` } } : undefined;
    const result = method === "GET" ? await client.get(path, auth) : await client.post(path, body, auth);
    console.log(JSON.stringify(result.data, null, json ? undefined : 2));
    if (result.status >= 400) process.exitCode = result.status >= 500 ? 2 : 1;
  }
  command.command("configure").description("Apply an explicit repository, assignee and target configuration")
    .requiredOption("--file <path>", "JSON configuration file; no credentials")
    .option("--json", "JSON output")
    .action(async (options: { file: string; json?: boolean }) => {
      const config: unknown = JSON.parse(readFileSync(options.file, "utf8"));
      await request("POST", "/api/github-intake/configure", config, options.json);
    });
  command.command("status").description("Read intake errors, assignments and pickup evidence")
    .option("--json", "JSON output")
    .action((options: { json?: boolean }) => request("GET", "/api/github-intake", undefined, options.json));
  command.command("sync").description("Reconcile GitHub and native queue state once")
    .option("--json", "JSON output")
    .action((options: { json?: boolean }) => request("POST", "/api/github-intake/reconcile", {}, options.json));
  command.command("eligibility").description("Record the manager's ownership and dependency checks for an issue revision")
    .requiredOption("--file <path>", "JSON issue revision, ready/blocked decision and reason")
    .option("--json", "JSON output")
    .action(async (options: { file: string; json?: boolean }) => {
      const decision: unknown = JSON.parse(readFileSync(options.file, "utf8"));
      await request("POST", "/api/github-intake/eligibility", decision, options.json);
    });
  return command;
}
