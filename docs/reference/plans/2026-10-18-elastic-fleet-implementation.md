# Elastic Fleet Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans task by task. Preserve the approved design and record progress.

**Goal:** Add opt-in, durable demand planning and Azure worker provisioning/draining alongside existing GitHub intake and human responses.
**Architecture:** Native intake remains task authority. A deterministic fleet planner calculates capacity; a durable reconciler records operations before calling an optional Azure adapter. Native routes/CLI expose status, planning, pause and explicit configuration. No cloud mutation occurs in dry-run.
**Tech Stack:** Existing TypeScript, SQLite, Hono, Commander, Vitest; Azure CLI as optional adapter dependency.
**Spec:** docs/reference/plans/2026-10-18-elastic-fleet-and-messaging-design.md

## Global Constraints

- Preserve the three permanent machines and existing writers. Default disabled/dry-run.
- Unknown observations never authorize capacity removal or duplicated issue execution.
- Two consecutive fresh 60-second demand observations before scale-out; 15-minute idle window before scale-in.
- Count booting capacity; two issue slots per elastic worker is a proposed configuration default, not deployment approval.
- No new fleet-wide bearer on workers, no public listener, no shared SQLite, no mandatory Azure dependency.
- Use docs/reference/plans because the repository docs guard rejects docs/superpowers paths.
- Upstream scope proposal: https://github.com/mvschwarz/openrig/issues/1442. Local development is authorized; upstream placement remains subject to maintainer review.

## Review Focus

- Stale/future telemetry must not become free capacity or a removal proof.
- Repeated ticks/API uncertainty must not create extra instances or duplicate assignments.
- Shared capability pools must not count one issue repeatedly.
- Dirty/unpushed work, active subprocesses and unresolved human return custody block deletion.
- Credential-bearing bootstrap output must never appear in API responses or ordinary logs.

### Task 1: Deterministic capacity policy

Files: create packages/daemon/src/domain/elastic-fleet-policy.ts and packages/daemon/test/elastic-fleet-policy.test.ts.
Interfaces: export FleetPolicy, FleetWorkerObservation, FleetDemand, FleetSnapshot, FleetPlan and planFleet(policy,snapshot,previous?) with a deterministic serializable result. Policy: poolId, enabled, mode dry-run|active, maxWorkers (elastic only), slotsPerWorker, capabilities, pollIntervalSeconds, scaleOutSamples, idleSeconds. Snapshot: observedAt Unix milliseconds, now Unix milliseconds, demands [{id,capability}], workers [{id,managed,permanent,state provisioning|ready|draining|quarantined,capabilities,slots,active,observedAt,idleSince,clean,pendingHumanReturns}], providerAvailable. Previous: lastObservedAt, demandSamples. Plan exposes add, drainIds, reason(s), demandSamples, observedAt, eligibleDemand and freeSlots.
- [ ] Write failing tests for disabled, dry-run proposed actions, demand filtering/deduplication, boot reservations, hard maximum, stale/future telemetry, two distinct 60-second samples, one-at-a-time idle drain and all unsafe drain barriers.
- [ ] Run focused Vitest; verify expected missing behavior fails.
- [ ] Implement policy with strict finite integer/enum validation and no IO. Unknown dirty/activity/human state fails closed. Require samples spaced by configured interval. Existing draining/provisioning hosts count toward ceiling.
- [ ] Run policy tests and typecheck. Commit feat(daemon): plan elastic capacity from eligible demand.

### Task 2: Durable pool lifecycle

Files: domain/elastic-fleet.ts, db/migrations/116_elastic_fleet.ts, db/all-migrations.ts, test/elastic-fleet.test.ts.
Interfaces: ElasticFleetService(db,io,now), configure(config: ElasticFleetConfiguration), status(), plan(), reconcile(), pause(), requestDrain(workerId). Export ElasticFleetConfiguration {policy: FleetPolicy, provider?: Record<string, unknown>}; reject secrets/provider invalid input via optional IO validateConfiguration(config). IO: observe(policy):Promise<FleetSnapshot>, provision({operationId,count,policy}):Promise<void>, beginDrain({operationId,workerId,policy}):Promise<void>, remove({operationId,workerId,policy}):Promise<void>, inspectOperation(operation: FleetOperation,config: ElasticFleetConfiguration):Promise<"succeeded"|"pending"|"unknown">. IO mutation methods also receive configuration and persisted snapshot/target capacity in the operation (absolute desired capacity, never count+current on replay). Persist configuration revision, sample history, operation id/type/state, worker id/count, error and timestamps. IO is trusted provider/native observation, never request-body telemetry.
- [ ] Write failing restart/readback, durable operation intent, dry-run no mutation, serialization/racing connections, pause/drain, provider uncertainty and unsafe deletion tests.
- [ ] Implement atomic reservation before external IO; retain uncertain operations and reconcile/readback before any new operation. Bound recorded errors and redact adapter output. Do not expire a running writer lock into duplicate work. A crash with uncertain ownership requires explicit reconciliation, not silent replay. Pending operation inspection may confirm completion; unknown never replays a mutation. Gate concurrent reconcile calls across SQLite connections with an atomic durable owner. Provide releaseReconcile({owner,operatorConfirmedStopped:true}) only for explicit operator recovery; do not expose automatic TTL takeover. Serialize pause/configure against operation reservation; active IO already in flight cannot be retroactively cancelled. Persist draining exclusion before calling beginDrain and expose drainingWorkerIds() for native dispatch admission; removal needs a fresh complete snapshot with zero active work, clean true, pendingHumanReturns false, same managed nonpermanent instance and confirmed completed beginDrain. Do not delete on absent/unknown observation. status has error:string|null and config with policy poll interval accessible for scheduler, or scheduler wrapper maps it.
- [ ] Run focused tests; commit durable lifecycle.

### Task 3: Azure adapter and native observation

Files: adapters/azure-elastic-fleet.ts, adapters/elastic-fleet.ts, tests/azure-elastic-fleet.test.ts, tests/elastic-fleet-runtime.test.ts.
Interfaces: createElasticFleetIO({githubIntake,hostsPath,queue,...}) returns Task2 IO. Azure provider config points to an explicitly owned dedicated scale set and pinned bootstrap manifest; use argv-based execFile, no shell interpolation of external identifiers. Native runtime observes actual queue/host resources and eligible intake revisions, not raw open issue count. Permanent/manual hosts never become deletion candidates. Managed workers need full boot/native readiness before advertising slots.
- [ ] Write failing tests for resource scope/tag mismatch, exact owned instance operations, provider unavailable, partial inventory, ambiguous scale result, no credential output, clean/claim checks and bootstrap pin mismatch.
- [ ] Implement Azure CLI provider operations guarded by exact resource identity and ownership tags. Preserve scale-in protection until a fresh native drain proof, delete only one exact instance. New workers are admitted only by trusted controller bootstrap/native registration; refuse activation until that bootstrap is configured and validated. Never approximate enrollment by trusting a Service Bus message.
- [ ] Run adapters and policy/service tests; commit Azure adapter.

### Task 4: Native API, CLI and scheduler integration

Files: routes/elastic-fleet.ts, server.ts, startup.ts, index.ts; CLI commands/elastic.ts, index.ts, lazy loader; matching API/CLI tests and runtime tests.
Interfaces: rig elastic configure --file, status, plan, sync, pause, drain <worker>. Authenticated mutation routes /api/elastic-fleet. Read plans use current native observations. Scheduler obeys explicit enabled/mode settings and shuts down cleanly. Configuration has a versioned provider block, no secret values. Unknown provider config fails closed.
- [ ] Write failing native route/CLI tests for auth, invalid config, JSON output, dry-run, command registration and lifecycle.
- [ ] Implement routes/CLI/startup/shutdown and use native registered ready hosts as dispatch targets while excluding draining hosts. Keep manual intake targets unchanged.
- [ ] Run affected suites/build/lint. Commit integrated controls.

### Task 5: Operational docs, review and acceptance

Files: docs/reference/elastic-fleet.md, integrations/azure-elastic/ bootstrap/IaC examples if provider support requires them; tests for runnable examples.
- [ ] Document exact active setup, limits, private networking, bootstrap/credential scope, dry-run rollout, pause, reconcile uncertainty and retained unsafe drains. State commands that are not shipped rather than implying availability.
- [ ] Independent Astra high whole-change review; correct material findings with regression tests.
- [ ] Run canonical repository/build/lint/workspace/UI checks, reuse only unchanged evidence explicitly and record any environmental failures.
- [ ] Run safe local/package roundtrip and live read-only three-VM demand plan. New-resource creation requires configured bounded deployment/enrollment authority; do not fabricate live cloud acceptance.
- [ ] Publish focused official-project PR(s) with exact head, test receipts, scope-issue link and explicit remaining live acceptance gates. Attach every created PR to this task.
