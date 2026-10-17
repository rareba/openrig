import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { hasLiveHumanGate, QUEUE_PRIORITIES, type QueuePriority, type QueueCreateInput, type QueueRepository } from "./queue-repository.js";

export interface GithubIntakeTarget {
  host: string;
  session: string;
  physicalHost: string;
  capacity: number;
  capabilities: string[];
}
export interface GithubIntakeConfig {
  enabled: boolean;
  assignee: string;
  repositories: Array<{ id: string; name: string }>;
  targets: GithubIntakeTarget[];
  hostCapacity: number;
  pollIntervalSeconds?: number;
  manager?: { host: string; session: string };
}
export interface GithubIssue {
  repositoryId: string;
  /** GitHub numeric ID and node_id, verified together by the adapter. */
  repositoryAliases?: string[];
  repository: string;
  number: number;
  url: string;
  title: string;
  body: string;
  updatedAt: string;
  state: "open" | "closed";
  assignees: string[];
}
export interface GithubTask {
  qitemId: string;
  state: string;
  destinationSession: string;
  blockedOn?: string | null;
  claimedAt?: string | null;
  waitingForHuman?: boolean;
  humanIntent?: "decision" | "update" | null;
  handedOffTo?: string | null;
  handedOffFrom?: string | null;
  closureReason?: string | null;
  closureTarget?: string | null;
}
export interface GithubTargetObservation {
  host: string;
  session: string;
  available: boolean;
  /** All active native queue work on this target, including intake assignments. */
  active: number;
  /** Occupied execution seats on this transport, including unconfigured seats. null is unknown. */
  hostActive?: number | null;
}
export interface GithubIntakeIO {
  /** A complete paginated snapshot; partial results and unavailable GitHub must throw. */
  listIssues(config: GithubIntakeConfig): Promise<GithubIssue[]>;
  observeTargets(config: GithubIntakeConfig): Promise<GithubTargetObservation[]>;
  /** null means confirmed absent; unknown/unreachable must throw. */
  getTask(host: string, id: string): Promise<GithubTask | null>;
  /** Resolve exactly one native handedOffFrom successor; absent/ambiguous must return null/throw. */
  findSuccessor?(host: string, task: GithubTask): Promise<{ host: string; task: GithubTask } | null>;
  createTask(host: string, input: QueueCreateInput & { qitemId: string }): Promise<GithubTask>;
}
export interface GithubEligibilityRequest {
  repositoryId: string;
  number: number;
  revision: string;
  eligibility: "ready" | "blocked";
  capability?: string;
  reason: string;
  priority?: QueuePriority;
}
interface Row {
  repository_id: string; issue_number: number; issue_json: string; eligibility: string; priority: QueuePriority; first_seen: string;
  approved_revision: string | null; capability: string | null; reason: string | null;
  in_scope: number; generation: number; qitem_id: string | null; target_host: string | null;
  target_session: string | null; target_machine: string | null; current_host: string | null; current_session: string | null; stage: string | null;
  delivery: string | null; pickup: string | null; error: string | null;
}
const runs = new WeakMap<Database.Database, Promise<unknown>>();
const text = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
const positive = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) > 0;
const errorText = (error: unknown) => error instanceof Error ? error.message : String(error);
const targetKey = (target: { host: string; session: string }) => `${target.host}\0${target.session}`;
const terminal = (task: GithubTask) => ["done", "canceled", "denied", "failed"].includes(task.state);
const waiting = (task: GithubTask) => task.waitingForHuman === true || hasLiveHumanGate({ ...task, blockedOn: task.blockedOn ?? null });

/** A source/custody index only: execution and human waits remain native queue items. */
export class GithubIntakeService {
  constructor(private db: Database.Database, readonly queue: QueueRepository, private io: GithubIntakeIO, private now = () => new Date()) {}

  configure(config: GithubIntakeConfig): ReturnType<GithubIntakeService["status"]> {
    if (!config || typeof config.enabled !== "boolean" || !text(config.assignee) || !positive(config.hostCapacity)
      || (config.pollIntervalSeconds !== undefined && (!positive(config.pollIntervalSeconds) || config.pollIntervalSeconds < 10))
      || !Array.isArray(config.repositories) || !config.repositories.length || !Array.isArray(config.targets)
      || config.repositories.some(r => !text(r.id) || !/^[\w.-]+\/[\w.-]+$/.test(r.name))
      || new Set(config.repositories.map(r => r.id)).size !== config.repositories.length
      || new Set(config.repositories.map(r => r.name.toLowerCase())).size !== config.repositories.length
      || config.targets.some(t => !text(t.host) || !text(t.session) || !text(t.physicalHost) || !positive(t.capacity)
        || !Array.isArray(t.capabilities) || t.capabilities.some(c => !text(c)))
      || new Set(config.targets.map(targetKey)).size !== config.targets.length
      || config.targets.some(t => config.targets.some(other => other.host === t.host && other.physicalHost !== t.physicalHost))
      || (config.manager && !config.targets.some(t => t.host === config.manager!.host && t.session === config.manager!.session))) {
      throw new Error("Invalid GitHub intake configuration; manager must be a configured target");
    }
    this.db.prepare("INSERT INTO github_intake_config(id,config) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET config=excluded.config,revision=github_intake_config.revision+1").run(JSON.stringify(config));
    return this.status();
  }

  status() {
    const settings = this.db.prepare("SELECT config,error,last_poll FROM github_intake_config WHERE id=1").get() as { config: string; error: string | null; last_poll: string | null } | undefined;
    return {
      config: settings ? JSON.parse(settings.config) as GithubIntakeConfig : null,
      error: settings?.error ?? null,
      lastPoll: settings?.last_poll ?? null,
      lastError: settings?.error ?? null,
      lastRun: settings?.last_poll ?? null,
      issues: this.rows().map(row => ({ issue: JSON.parse(row.issue_json) as GithubIssue, eligibility: row.eligibility,
        approvedRevision: row.approved_revision, reason: row.reason, priority: row.priority, firstSeen: row.first_seen, inScope: !!row.in_scope,
        qitemId: row.qitem_id, host: row.target_host, session: row.target_session, currentHost: row.current_host, currentSession: row.current_session, stage: row.stage,
        delivery: row.delivery, pickup: row.pickup, error: row.error })),
    };
  }

  markEligibility(request: GithubEligibilityRequest): ReturnType<GithubIntakeService["status"]> {
    const row = this.rows().find(r => r.repository_id === request.repositoryId && r.issue_number === request.number);
    if (!row || !row.in_scope || (JSON.parse(row.issue_json) as GithubIssue).updatedAt !== request.revision
      || (request.priority !== undefined && !QUEUE_PRIORITIES.includes(request.priority))
      || !["ready", "blocked"].includes(request.eligibility) || !text(request.reason)
      || (request.eligibility === "ready" && !text(request.capability))) {
      throw new Error("Issue must be in scope at the reviewed revision, with reason and ready capability");
    }
    // An approval cannot silently take custody away from an existing worker or live triage task.
    if (row.qitem_id && !(row.stage === "triage" && row.pickup === "terminal")) {
      throw new Error("Finish the current native queue task before changing eligibility");
    }
    this.db.prepare(`UPDATE github_intake_issues SET eligibility=?,approved_revision=?,capability=?,reason=?,priority=?,
      qitem_id=NULL,target_host=NULL,target_session=NULL,target_machine=NULL,current_host=NULL,current_session=NULL,stage=NULL,delivery=NULL,pickup=NULL,error=NULL
      WHERE repository_id=? AND issue_number=?`).run(request.eligibility, request.revision, request.capability ?? null, request.reason, request.priority ?? "routine", request.repositoryId, request.number);
    this.db.prepare("UPDATE github_intake_config SET revision=revision+1 WHERE id=1").run();
    return this.status();
  }

  reconcile(): Promise<ReturnType<GithubIntakeService["status"]>> {
    const previous = runs.get(this.db) ?? Promise.resolve();
    const current = previous.catch(() => {}).then(() => this.run());
    runs.set(this.db, current);
    void current.finally(() => { if (runs.get(this.db) === current) runs.delete(this.db); }).catch(() => {});
    return current;
  }

  private rows(): Row[] {
    return this.db.prepare("SELECT * FROM github_intake_issues ORDER BY CASE WHEN eligibility='ready' THEN 0 ELSE 1 END, CASE priority WHEN 'critical' THEN 0 WHEN 'urgent' THEN 1 ELSE 2 END,first_seen,repository_id,issue_number").all() as Row[];
  }
  private update(row: Row, values: Partial<Row>) {
    const keys = Object.keys(values) as Array<keyof Row>;
    this.db.prepare(`UPDATE github_intake_issues SET ${keys.map(k => `${k}=?`).join(",")} WHERE repository_id=? AND issue_number=?`)
      .run(...keys.map(k => values[k] ?? null), row.repository_id, row.issue_number);
    Object.assign(row, values);
  }
  private revision(): number {
    return (this.db.prepare("SELECT revision FROM github_intake_config WHERE id=1").get() as { revision: number } | undefined)?.revision ?? -1;
  }
  private assertCurrent(revision: number) {
    if (revision !== this.revision()) throw new Error("Reconciliation superseded by configuration or eligibility change");
  }
  private async run() {
    const revision = this.revision();
    const config = this.status().config;
    if (!config?.enabled) return this.status();
    try {
      const issues = await this.io.listIssues(config);
      this.assertCurrent(revision);
      if (!Array.isArray(issues) || issues.some(issue => !config.repositories.some(r => (r.id === issue.repositoryId || issue.repositoryAliases?.includes(r.id)) && r.name.toLowerCase() === issue.repository.toLowerCase())
        || (issue.repositoryAliases !== undefined && (!Array.isArray(issue.repositoryAliases) || issue.repositoryAliases.some(alias => !text(alias)) || !issue.repositoryAliases.includes(issue.repositoryId)))
        || !positive(issue.number) || !text(issue.updatedAt) || !text(issue.title) || typeof issue.body !== "string"
        || !["open", "closed"].includes(issue.state) || !Array.isArray(issue.assignees) || issue.assignees.some(a => !text(a))
        || typeof issue.url !== "string" || issue.url !== `https://github.com/${issue.repository}/issues/${issue.number}`)) throw new Error("GitHub returned invalid or out-of-scope issues");
      this.db.transaction(() => {
        this.assertCurrent(revision);
        this.db.prepare("UPDATE github_intake_issues SET in_scope=0").run();
        const upsert = this.db.prepare(`INSERT INTO github_intake_issues(repository_id,issue_number,issue_json,in_scope,first_seen) VALUES(?,?,?,?,?)
          ON CONFLICT(repository_id,issue_number) DO UPDATE SET issue_json=excluded.issue_json,in_scope=excluded.in_scope`);
        for (const issue of issues) {
          const aliases = this.rows().filter(row => row.issue_number === issue.number && (JSON.parse(row.issue_json) as GithubIssue).repository.toLowerCase() === issue.repository.toLowerCase());
          if (aliases.length > 1) throw new Error("Conflicting existing repository source identities require custody reconciliation");
          const existing = aliases[0];
          if (existing && existing.repository_id !== issue.repositoryId) {
            if (!issue.repositoryAliases?.includes(existing.repository_id)) throw new Error("Repository source identity changed without a verified alias");
            this.db.prepare("UPDATE github_intake_issues SET repository_id=? WHERE repository_id=? AND issue_number=?").run(issue.repositoryId, existing.repository_id, issue.number);
          }
          upsert.run(issue.repositoryId, issue.number, JSON.stringify(issue), Number(issue.state === "open" && issue.assignees.some(a => a.toLowerCase() === config.assignee.toLowerCase())), this.now().toISOString());
        }
        this.db.prepare(`UPDATE github_intake_issues SET eligibility='triage',priority='routine',approved_revision=NULL,capability=NULL,reason='Source revision changed; review again'
          WHERE qitem_id IS NULL AND approved_revision IS NOT NULL AND approved_revision != json_extract(issue_json,'$.updatedAt')`).run();
      })();
      const observations = await this.io.observeTargets(config);
      this.assertCurrent(revision);
      if (!Array.isArray(observations) || observations.some(o => typeof o.available !== "boolean" || !Number.isSafeInteger(o.active) || o.active < 0 || (o.hostActive !== undefined && o.hostActive !== null && (!Number.isSafeInteger(o.hostActive) || o.hostActive < 0)))) throw new Error("Invalid host observations");
      const rows = this.rows();
      // Readback first: timeout reservations survive restarts and never move to a different target.
      const tasks = new Map<string, GithubTask>();
      const unknown = new Set<string>();
      for (const row of rows.filter(r => r.qitem_id)) {
        try {
          let task = await this.io.getTask(row.target_host!, row.qitem_id!);
          this.assertCurrent(revision);
          if (task && task.qitemId !== row.qitem_id) throw new Error("Queue readback returned the wrong task");
          let currentHost = row.target_host!;
          const seen = new Set<string>();
          while (task && (task.state === "handed-off" || task.closureReason === "handed_off_to" || task.handedOffTo)) {
            const key = `${currentHost}:${task.qitemId}`;
            if (seen.has(key)) throw new Error("Queue handoff cycle");
            seen.add(key);
            const source = task;
            const remoteSuccessor = /^(qitem-[^@]+)@([^@]+)$/.exec(source.closureTarget ?? "");
            if (remoteSuccessor) {
              currentHost = remoteSuccessor[2]!;
              task = await this.io.getTask(currentHost, remoteSuccessor[1]!);
              this.assertCurrent(revision);
              if (!task || task.qitemId !== remoteSuccessor[1]) throw new Error("Queue cross-host successor unknown");
            } else {
              const successor = await this.io.findSuccessor?.(currentHost, source);
              this.assertCurrent(revision);
              if (!successor || successor.task.handedOffFrom !== source.qitemId) throw new Error("Queue native successor unknown or invalid");
              currentHost = successor.host; task = successor.task;
            }
          }
          if (task) {
            tasks.set(row.qitem_id!, task);
            this.update(row, { current_host: currentHost, current_session: task.destinationSession, delivery: "delivered", pickup: terminal(task) ? "terminal" : waiting(task) ? "waiting-human" : task.claimedAt ? "claimed" : "unclaimed", error: null });
          } else if (row.delivery === "delivered") throw new Error("Previously delivered queue task is missing; inspect custody before retrying");
        } catch (error) {
          unknown.add(row.qitem_id!); this.update(row, { error: errorText(error), pickup: "unknown" });
        }
      }
      const usage = new Map<string, number>();
      for (const target of config.targets) {
        const observed = observations.find(o => targetKey(o) === targetKey(target));
        const assigned = rows.filter(r => r.qitem_id && (r.current_host ?? r.target_host) === target.host && (r.current_session ?? r.target_session) === target.session);
        const knownActive = assigned.filter(r => tasks.has(r.qitem_id!) && !terminal(tasks.get(r.qitem_id!)!) && !waiting(tasks.get(r.qitem_id!)!)).length;
        const uncertain = assigned.filter(r => !tasks.has(r.qitem_id!)).length;
        // Confirmed native tasks are already observed; unconfirmed reservations consume session capacity.
        usage.set(targetKey(target), Math.max(observed?.active ?? 0, knownActive) + uncertain);
      }
      const machineUsage = (machine: string) => {
        const targets = config.targets.filter(t => t.physicalHost === machine);
        let total = 0;
        for (const host of new Set(targets.map(t => t.host))) {
          const hostTargets = targets.filter(t => t.host === host);
          const hostObservations = observations.filter(o => o.host === host);
          if (!hostObservations.length || hostObservations.some(o => o.hostActive === null)) return Infinity;
          const observedTotal = hostObservations.some(o => o.hostActive !== undefined)
            ? Math.max(...hostObservations.map(o => o.hostActive ?? 0))
            : hostTargets.reduce((n, t) => n + Math.min(1, observations.find(o => targetKey(o) === targetKey(t))?.active ?? 0), 0);
          // Pending packets share a seat; only its first reservation adds physical occupancy.
          const delta = hostTargets.reduce((n, t) => n + Math.max(0, Math.min(1, usage.get(targetKey(t)) ?? 0) - Math.min(1, observations.find(o => targetKey(o) === targetKey(t))?.active ?? 0)), 0);
          total += observedTotal + delta;
        }
        return total;
      };
      for (const row of rows) {
        this.assertCurrent(revision);
        const issue = JSON.parse(row.issue_json) as GithubIssue;
        if (!row.in_scope) { this.update(row, { error: "Source closed, reassigned or unavailable in authorized scope; existing custody retained" }); continue; }
        if (row.qitem_id && row.approved_revision && row.approved_revision !== issue.updatedAt) {
          this.update(row, { error: "Source revision superseded approval; existing custody retained for review" }); continue;
        }
        if (row.qitem_id) {
          if (unknown.has(row.qitem_id) || tasks.has(row.qitem_id)) continue;
          // A confirmed absent reservation can retry only under the same current scope/revision.
          if (row.stage === "ready" && row.approved_revision !== issue.updatedAt) continue;
          const target = config.targets.find(t => t.host === row.target_host && t.session === row.target_session);
          const observed = target && observations.find(o => targetKey(o) === targetKey(target));
          if (!target || !observed?.available) continue;
          const occupied = machineUsage(target.physicalHost);
          const occupiedManagerTriage = row.stage === "triage" && target.host === config.manager?.host
            && target.session === config.manager.session && observed.active > 0 && Number.isFinite(occupied);
          if ((usage.get(targetKey(target)) ?? 0) > target.capacity || (!occupiedManagerTriage && occupied > config.hostCapacity)) {
            this.update(row, { error: "Reserved retry waiting for target and physical host capacity" }); continue;
          }
          await this.deliver(row, issue, revision); continue;
        }
        const stage = row.eligibility === "ready" && row.approved_revision === issue.updatedAt ? "ready" : row.eligibility === "triage" ? "triage" : null;
        if (!stage) continue;
        const target = config.targets.find(t => {
          if (stage === "triage" ? t.host !== config.manager?.host || t.session !== config.manager.session : !t.capabilities.includes(row.capability!)) return false;
          const observed = observations.find(o => targetKey(o) === targetKey(t));
          if (!observed?.available || (usage.get(targetKey(t)) ?? 0) >= t.capacity) return false;
          const occupied = machineUsage(t.physicalHost);
          // Triage on an observed occupied manager adds no seat and can reconcile legacy overload.
          return Number.isFinite(occupied) && ((stage === "triage" && observed.active > 0)
            || occupied + ((usage.get(targetKey(t)) ?? 0) > 0 ? 0 : 1) <= config.hostCapacity);
        });
        if (!target) continue;
        const generation = row.generation + 1;
        const hash = createHash("sha256").update(`${row.repository_id}:${row.issue_number}:${generation}`).digest("hex").slice(0, 24);
        // This committed reservation is the outbox. Native queue create commits its own wake intent.
        this.db.transaction(() => {
          this.assertCurrent(revision);
          this.update(row, { qitem_id: `qitem-gh-${hash}`, target_host: target.host, target_session: target.session, target_machine: target.physicalHost, current_host: target.host, current_session: target.session, generation, stage, delivery: "reserved", pickup: "unknown", error: null });
        })();
        usage.set(targetKey(target), (usage.get(targetKey(target)) ?? 0) + 1);
        await this.deliver(row, issue, revision);
      }
      this.assertCurrent(revision);
      this.db.prepare("UPDATE github_intake_config SET error=NULL,last_poll=? WHERE id=1").run(this.now().toISOString());
    } catch (error) {
      if (this.revision() === revision) this.db.prepare("UPDATE github_intake_config SET error=? WHERE id=1").run(errorText(error));
    }
    return this.status();
  }
  private async deliver(row: Row, issue: GithubIssue, revision: number) {
    this.db.transaction(() => {
      this.assertCurrent(revision);
      const current = this.rows().find(r => r.repository_id === row.repository_id && r.issue_number === row.issue_number);
      if (!current?.in_scope || current.qitem_id !== row.qitem_id || (current.stage === "ready" && (current.eligibility !== "ready" || current.approved_revision !== issue.updatedAt))) {
        throw new Error("Dispatch superseded by source or approval change");
      }
    })();
    const triageDecision = JSON.stringify({ repositoryId: issue.repositoryId, number: issue.number, revision: issue.updatedAt, eligibility: "ready", capability: "<configured capability: review, release, acceptance or code>", priority: "routine", reason: "<exact next action; current ownership; branch/PR and dependency checks; downstream review/acceptance capacity; priority rationale>" }, null, 2);
    const triage = `Triage this issue; verify current ownership, branches/PRs, dependencies and downstream review/acceptance capacity. Prefer finishing existing review, release or acceptance over expanding implementation work. Do not implement yet.
Write a decision JSON file with this shape. Name the exact next action, current ownership, downstream review/acceptance capacity and priority rationale in reason. Set capability to the configured action capability (for example review, release, acceptance or code). Set native priority to routine, urgent or critical; this records manager priority only, never GitHub priority. Use eligibility blocked with a reason when unsafe:
${triageDecision}
Finish this native triage task first: rig queue update ${row.qitem_id} --state done --closure-reason no-follow-on --resolution "Triage decision saved with ownership, branch and dependency evidence" --json
Then run on the intake controller: rig github sync --json
Then record the decision: rig github eligibility --file <decision.json> --json
Then run: rig github sync --json
Use rig github status --json to verify custody and pickup; never bypass an existing writer.`;
    try {
      const task = await this.io.createTask(row.target_host!, { qitemId: row.qitem_id!, sourceSession: "github-intake", destinationSession: row.target_session!, priority: row.priority,
        body: `Proceed within the existing project rules when the next action is authorized. If a genuine missing fact or decision needs the operator, create a native human@kernel decision request with summary, evidenceRef and structured humanQuestions (2-4 choices, a recommendation and consequences when useful). Park only its dependent action; retain issue custody and continue independent eligible work. Never infer approval from silence.\n\n${row.stage === "triage" ? triage : `Carry out the manager-reviewed next action, preserving existing writers and project acceptance gates. Do not duplicate implementation when a branch or PR already exists.\nCapability: ${row.capability}\nManager next action and evidence: ${row.reason}`}\n${issue.url}\n${issue.title}\n\n${issue.body}\n\nSource revision: ${issue.updatedAt}\nGitHub issue closure requires separate acceptance; queue completion does not close it.`,
        tags: ["github-intake", `github:${issue.repositoryId}:${issue.number}`] });
      if (task.qitemId !== row.qitem_id) throw new Error("Queue did not preserve the reserved ID");
      this.update(row, { delivery: "delivered", pickup: task.claimedAt ? "claimed" : "unclaimed", error: null });
    } catch (error) { this.update(row, { delivery: "unknown", error: errorText(error) }); }
  }
}
