import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { GithubIntakeConfig, GithubIntakeIO, GithubIssue, GithubTask } from "../domain/github-intake.js";
import { isHumanSeatSessionRef } from "../domain/session-name.js";
import { hasLiveHumanGate, type QueueRepository } from "../domain/queue-repository.js";
import { loadHostRegistry, resolveHost, type HttpHostEntry } from "../domain/hosts/hosts-registry-reader.js";
import { remoteJsonRequest, type RemoteJsonOptions, type RemoteJsonResult } from "../domain/hosts/remote-daemon-http.js";

export type GithubCommand = (command: string, args: string[]) => Promise<string>;
const execute = promisify(execFile);
const runGithub: GithubCommand = async (command, args) => (await execute(command, args, { timeout: 60_000, maxBuffer: 32 * 1024 * 1024 })).stdout;
function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Expected an object from the remote service");
  return value as Record<string, unknown>;
}

/** A failed page invalidates the entire snapshot; never return a partial backlog. */
export async function readGithubIssues(config: GithubIntakeConfig, run: GithubCommand = runGithub): Promise<GithubIssue[]> {
  const issues: GithubIssue[] = [];
  for (const repository of config.repositories) {
    const metadata = record(JSON.parse(await run("gh", ["api", `repos/${repository.name}`])));
    if (!Number.isSafeInteger(metadata.id) || Number(metadata.id) < 1) throw new Error(`Invalid GitHub repository identity: ${repository.name}`);
    if (String(metadata.id) !== repository.id && metadata.node_id !== repository.id) throw new Error(`Repository identity changed: ${repository.name}`);
    const pages: unknown = JSON.parse(await run("gh", ["api", "--paginate", "--slurp", `repos/${repository.name}/issues?state=open&assignee=${encodeURIComponent(config.assignee)}&per_page=100`]));
    if (!Array.isArray(pages) || !pages.length || pages.some(page => !Array.isArray(page))) throw new Error(`Invalid GitHub page stream: ${repository.name}`);
    for (const page of pages) for (const value of page as unknown[]) {
      const item = record(value);
      if (item.pull_request) continue;
      if (!Number.isSafeInteger(item.number) || Number(item.number) < 1 || typeof item.title !== "string"
        || typeof item.html_url !== "string" || typeof item.updated_at !== "string" || !Array.isArray(item.assignees)
        || (item.body !== null && typeof item.body !== "string") || !["open", "closed"].includes(String(item.state))) throw new Error(`Invalid GitHub issue in ${repository.name}`);
      const assignees = item.assignees.map(value => {
        const assignee = record(value);
        if (typeof assignee.login !== "string") throw new Error(`Invalid issue assignee in ${repository.name}`);
        return assignee.login;
      });
      issues.push({ repositoryId: String(metadata.id), repositoryAliases: [String(metadata.id), String(metadata.node_id ?? metadata.id)], repository: repository.name, number: Number(item.number),
        url: item.html_url, title: item.title, body: item.body ?? "", updatedAt: item.updated_at,
        state: item.state as "open" | "closed", assignees });
    }
  }
  return issues;
}

type NodeObservation = { archived?: boolean; canonicalSessionName?: string | null; sessionStatus?: string | null; startupStatus?: string | null;
  lifecycleState?: string; terminalActive?: boolean | null; heldReason?: unknown; agentActivity?: { state?: string } | null };
interface RuntimeOptions {
  queue: QueueRepository;
  localNodes: () => NodeObservation[] | Promise<NodeObservation[]>;
  hostsPath?: string;
  runGithub?: GithubCommand;
  request?: (host: HttpHostEntry, path: string, options: RemoteJsonOptions) => Promise<RemoteJsonResult>;
  onError?: (message: string) => void;
}

/** Uses the existing host registry and bearer transport; no host-selection global state. */
export function createGithubIntakeIO(options: RuntimeOptions): GithubIntakeIO {
  function hostEntry(id: string): HttpHostEntry {
    const registry = loadHostRegistry(options.hostsPath);
    if (!registry.ok) throw new Error(registry.error);
    const resolved = resolveHost(registry.registry, id);
    if (!resolved.ok) throw new Error(resolved.error);
    if (resolved.host.transport !== "http") throw new Error(`GitHub intake requires an HTTP host: ${id}`);
    return resolved.host;
  }
  async function remote(host: string, path: string, method: "GET" | "POST", body?: unknown) {
    return (options.request ?? remoteJsonRequest)(hostEntry(host), path, {
      method, body, timeoutMs: method === "GET" ? 5_000 : 10_000, maxBytes: 16 * 1024 * 1024,
      headers: { "X-OpenRig-Session": "github-intake@system" },
    });
  }
  function rawTask(value: unknown): GithubTask {
    const row = record(value);
    if (typeof row.qitemId !== "string" || typeof row.state !== "string" || typeof row.destinationSession !== "string"
      || (row.blockedOn != null && typeof row.blockedOn !== "string")
      || (row.humanIntent != null && row.humanIntent !== "decision" && row.humanIntent !== "update")) throw new Error("Invalid native queue task response");
    return { qitemId: row.qitemId, state: row.state, destinationSession: row.destinationSession,
      blockedOn: typeof row.blockedOn === "string" ? row.blockedOn : null,
      humanIntent: row.humanIntent === "update" ? "update" : row.humanIntent === "decision" ? "decision" : null,
      claimedAt: typeof row.claimedAt === "string" ? row.claimedAt : null,
      handedOffTo: typeof row.handedOffTo === "string" ? row.handedOffTo : null,
      handedOffFrom: typeof row.handedOffFrom === "string" ? row.handedOffFrom : null,
      closureTarget: typeof row.closureTarget === "string" ? row.closureTarget : null,
      closureReason: typeof row.closureReason === "string" ? row.closureReason : null,
      waitingForHuman: hasLiveHumanGate({ humanIntent: typeof row.humanIntent === "string" ? row.humanIntent : null, state: row.state, destinationSession: row.destinationSession, blockedOn: typeof row.blockedOn === "string" ? row.blockedOn : null }) };
  }
  async function readRawTask(host: string, id: string): Promise<GithubTask | null> {
    let value: unknown;
    if (host === "local") {
      value = options.queue.getById(id);
      if (!value) return null;
    } else {
      const result = await remote(host, `/api/queue/${encodeURIComponent(id)}`, "GET");
      if (!result.ok) {
        if (result.kind === "http" && result.status === 404) return null;
        throw new Error(`Queue read unavailable for ${host}: ${result.kind}`);
      }
      value = result.payload;
    }
    const row = rawTask(value);
    if (row.qitemId !== id) throw new Error("Queue readback returned the wrong task");
    return row;
  }
  async function task(host: string, value: unknown): Promise<GithubTask> {
    const row = rawTask(value);
    // Native block --on <qitem> retains the worker row; inspect its direct gate on this host once.
    if (!row.waitingForHuman && row.humanIntent !== "update" && row.state === "blocked" && row.blockedOn?.startsWith("qitem-")) {
      const blocker = await readRawTask(host, row.blockedOn);
      row.waitingForHuman = blocker !== null && hasLiveHumanGate({ ...blocker, blockedOn: blocker.blockedOn ?? null });
    }
    return row;
  }
  return {
    listIssues: config => readGithubIssues(config, options.runGithub),
    async getTask(host, id) {
      const row = await readRawTask(host, id);
      return row ? task(host, row) : null;
    },
    async createTask(host, input) {
      if (host === "local") return task(host, await options.queue.create(input));
      const result = await remote(host, "/api/queue/create", "POST", input);
      if (!result.ok) throw new Error(`Queue delivery uncertain for ${host}: ${result.kind}`);
      return task(host, result.payload);
    },
    async findSuccessor(host, source) {
      let values: unknown;
      if (host === "local") values = options.queue.list({ handedOffFrom: source.qitemId, limit: 2 });
      else {
        const result = await remote(host, `/api/queue/list?handedOffFrom=${encodeURIComponent(source.qitemId)}&limit=2`, "GET");
        if (!result.ok) throw new Error(`Handoff read unavailable: ${host}`);
        values = result.payload;
      }
      if (!Array.isArray(values) || values.length !== 1) throw new Error("Handoff must have exactly one native successor");
      const successor = await task(host, values[0]);
      if (successor.handedOffFrom !== source.qitemId || successor.destinationSession !== source.handedOffTo) throw new Error("Handoff lineage mismatch");
      return { host, task: successor };
    },
    async observeTargets(config) {
      const hosts = new Map<string, Promise<{ nodes: NodeObservation[]; work: Record<string, unknown>[] }>>();
      async function inventory(host: string) {
        let nodes: NodeObservation[];
        let work: unknown;
        if (host === "local") {
          nodes = await options.localNodes();
          work = options.queue.list({ state: ["pending", "in-progress"], limit: 200 });
        } else {
          const rigs = await remote(host, "/api/ps?includeArchived=true", "GET");
          if (!rigs.ok || !Array.isArray(rigs.payload)) throw new Error(`Host inventory unavailable: ${host}`);
          nodes = (await Promise.all(rigs.payload.map(async value => {
            const rig = record(value);
            if (typeof rig.rigId !== "string") throw new Error(`Invalid rig inventory: ${host}`);
            const result = await remote(host, `/api/rigs/${encodeURIComponent(rig.rigId)}/nodes`, "GET");
            if (!result.ok || !Array.isArray(result.payload)) throw new Error(`Node inventory unavailable: ${host}`);
            return result.payload.map(value => ({ ...record(value), archived: !!rig.archivedAt }) as NodeObservation);
          }))).flat();
          const result = await remote(host, "/api/queue/list?state=pending,in-progress&limit=200", "GET");
          if (!result.ok) throw new Error(`Work inventory unavailable: ${host}`);
          work = result.payload;
        }
        if (!Array.isArray(work) || work.length >= 200) throw new Error(`Incomplete work inventory: ${host}`);
        return { nodes, work: work.map(record).filter(row => typeof row.destinationSession === "string" && !isHumanSeatSessionRef(row.destinationSession)) };
      }
      return Promise.all(config.targets.map(async target => {
        try {
          if (!hosts.has(target.host)) hosts.set(target.host, inventory(target.host));
          const { nodes, work } = await hosts.get(target.host)!;
          const active = (session: string) => {
            const seats = nodes.filter(n => n.canonicalSessionName === session);
            const busy = seats.some(n => n.terminalActive === true || n.agentActivity?.state === "running"
              || (n.sessionStatus === "running" && n.terminalActive == null && (!n.agentActivity || n.agentActivity.state === "unknown")));
            return Math.max(work.filter(row => row.destinationSession === session).length, busy ? 1 : 0);
          };
          const sessions = new Set([...nodes.map(n => n.canonicalSessionName).filter((s): s is string => !!s),
            ...work.map(row => row.destinationSession).filter((s): s is string => typeof s === "string")]);
          const hostActive = [...sessions].reduce((sum, session) => sum + Math.min(1, active(session)), 0);
          const node = nodes.find(n => n.canonicalSessionName === target.session && !n.archived);
          const available = !!node && node.sessionStatus === "running" && node.startupStatus === "ready"
            && node.lifecycleState !== "attention_required" && !node.heldReason && node.agentActivity?.state !== "needs_input";
          return { host: target.host, session: target.session, available, active: active(target.session), hostActive };
        } catch (error) {
          options.onError?.(`GitHub target ${target.host}/${target.session}: ${error instanceof Error ? error.message : String(error)}`);
          return { host: target.host, session: target.session, available: false, active: 0, hostActive: null };
        }
      }));
    },
  };
}
