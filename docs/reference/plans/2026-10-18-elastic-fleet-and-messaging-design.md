# Elastic workers and durable human messaging

Status: proposal for user and upstream maintainer review; no new cloud resources deployed.

## Outcome

OpenRig should turn eligible GitHub issues into independently owned work across permanent
and temporary machines, add Azure capacity when useful work exceeds available capacity,
and remove temporary machines only after their work is safely finished or transferred.
People can answer questions from the native interface or an authorized messenger through
Hermes, including while the messenger is temporarily offline. Waiting for one person or
one dependency must leave unrelated eligible work running.

The user requested an official OpenRig contribution. The pasted architecture proposes
three permanent Azure workers, an elastic VM Scale Set, GitHub App intake and Service Bus.
The choices below refine that proposal; suggested limits are recommendations, not approved
cloud spending or permission grants.

## Existing foundations and constraints

At source revision 76ea466d, the feature branch already contains revision-aware GitHub
intake, native cross-host dispatch, durable human response handling, and a Hermes relay.
Relevant seams: `domain/github-intake.ts`, `domain/github-poll-scheduler.ts`,
`routes/hosts.ts`, `domain/human-relay.ts`, `routes/human-responses.ts`, and
`integrations/hermes-openrig/`. These are candidate changes, not all upstream features.

Live read-only Azure inventory on 2026-10-18 found three standalone D8as_v5 workers and
no OpenRig VMSS. Preserve these permanent workers and their state. Unrelated Azure scale
sets must never enter this controller's ownership scope. Existing services being online
does not prove productive work. Existing recovery owners remain responsible for repairs.

OpenRig has a daemon and local SQLite/filesystem on each host. Do not share its SQLite
database over a network filesystem or treat a stateless VM as durable task custody.
The CLI is primary; the web UI remains experimental and best-effort.

`CONTRIBUTING.md` asks for maintainer agreement before building integrations and one concern
per PR. Prepare a scope issue/Ideas proposal first, then a linked series of focused PRs.
Keep deployment names, account identities, tokens and private topology out of upstream code.

## One ownership ledger, optional delivery transports

Use one designated durable OpenRig coordinator as authority for intake identity, assignment,
lease generation and human-response lineage. Maintain it on persistent storage outside the
elastic pool, with tested backups. Coordinator outage pauses new assignments and scaling;
workers may finish their existing assignments and retain results until reconciliation.
Version one is single-writer and has no automatic coordinator failover.
After restoring a backup, remain paused until an operator reconciles worker assignments,
GitHub branches, response receipts and provider instances against the restored ledger.
Advance a durable fleet epoch and invalidate old enrollment grants/assignment authority
before resuming; a restore must never silently reissue an older assignment. Automatic
recovery from a rewound coordinator backup is outside version one.

GitHub webhooks provide low-latency hints; periodic complete, paginated reconciliation
repairs missed events and rejects partial snapshots. Authenticate deliveries and deduplicate
delivery IDs. A webhook cannot directly mark an issue eligible or grant agent permissions.
Use the existing reviewed-revision/capability eligibility gate and native queue semantics.

Service Bus is an optional transport adapter, not a second task scheduler or completion
ledger. An Azure Function may validate and forward webhook events where desired; it is not
required for the first working slice. Durable intake/outbox records support direct polling
without provisioning Service Bus. When enabled, settle broker messages after durable native
admission, not after an hours-long agent task. Redelivery must produce the same task.
Broker locks and duplicate-detection windows do not establish exclusive execution ownership.

## Deterministic dispatch and recovery

Represent an issue by immutable repository identity and issue number; track reviewed source
revision separately. Each execution attempt has an assignment ID, worker identity, lease
generation and idempotency key. Atomically reserve an eligible item and compatible capacity
on the coordinator before remote admission. After timeout, reconcile the same assignment
before attempting another admission. Multiple coordinator processes cannot assign from the
same ledger concurrently without the same transaction and leader fence.

Workers acknowledge admission separately from pickup, progress, checkpoint and completion.
Heartbeat interval: 30 seconds; stale after 120 seconds (proposed defaults). A stale worker
is quarantined for reconciliation, not immediately duplicated on another machine. Lease
expiry alone never proves an old writer stopped. Reassignment requires confirmed termination
or explicit handoff and a recoverable branch/checkpoint. GitHub writes, review and merge
continue through existing repository gates; an assignment lease cannot fence arbitrary git
pushes. Uncertain old-writer state holds that issue and allows other issues to proceed.

Persist pushed branch/commit, workspace identity, queue lineage and validation evidence at
checkpoints. If a VM disappears with unpushed work, record possible loss; do not claim it was
recovered. Only confirmed saved state is resumed. Reviewer independence is retained.

## Demand and scaling policy

Count eligible, dependency-ready, unowned work with matching capabilities. Exclude human
waits, blocked items, active claims, duplicate issues and work outside the configured scope.
Measure free healthy execution slots across permanent and elastic hosts; unknown telemetry
does not become free capacity. Track CPU/RAM/build pressure and shared provider availability
as additional admission limits. Running seats alone are not a productivity metric.

Proposed policy: three protected permanent hosts; zero to seven additional elastic hosts;
two issue slots per elastic worker initially. Maximum total ten hosts is a suggested ceiling.
With runnable backlog R, compatible free slots F, and S slots per new host, request
ceil(max(0, R-F)/S) additional hosts, bounded by the configured ceiling, reserved booting
capacity, provider concurrency and regional quota. Allocate per capability pool; avoid
counting the same multi-capability backlog in multiple pools.

Scale out after two consecutive fresh 60-second observations. Count booting hosts toward
desired capacity; use idempotent operation IDs and provider discovery after ambiguous API
results. Bound retries with exponential backoff and alert on quota, bootstrap and capacity
failures. A provider outage must not trigger repeated resource creation.

Scale in after 15 minutes with no assignments and surplus capacity. Drain one host at a time.
Do not infer safety from an empty GitHub queue. No automatic deletion of permanent hosts.
Budget enforcement is a separately configured cap using operator-provided price estimates;
without a configured deployment ceiling and provider permissions, scaling stays dry-run.
Observed Azure/model billing can lag; never advertise an exact hard spending guarantee.

## Azure adapter and trusted bootstrap

Define a narrow provider interface: inspect owned instances, ensure desired instance exists,
observe operation, protect/unprotect scale-in, delete a specifically drained instance.
Ship the Azure implementation and Bicep example separately from generic scheduling logic.
Disable independent Azure autoscale rules that could remove instances outside OpenRig drain.
Validate the selected VMSS mode/API supports the protection operations used by the adapter.

Bootstrap from a pinned image/build manifest: Node, tmux, OpenRig, supported agent runtimes,
repository tools and configured rig template. An instance is not schedulable until its
version, private connectivity, credentials, capabilities and a harmless native execution
probe pass. Configure cloud-init/systemd idempotently; never clone a live user's provider
session home, bearer or SQLite database into the image.

Use managed identity and least-privilege resource scope for Azure operations and secret
retrieval. An explicit fleet enrollment policy authorizes a pool, image/version, coordinator,
repository scope and maximum size. Verify instance identity through trusted Azure control
plane discovery plus authenticated instance proof, issue a short-lived single-use enrollment
grant bound to that instance, and register native host credentials using an audited path.
Service Bus membership or a self-asserted instance ID is insufficient. Existing manual host
pairing remains unchanged. Pool enrollment is a new explicit authorization surface, not
automatic approval of the existing human pairing question. Exact proof mechanism and
credential storage are implementation decisions requiring a threat review before enabling.

Workers use supported headless model authentication and narrowly scoped GitHub App access;
scope repositories and write permissions explicitly. No fleet-wide operator bearer on every
worker. Provider access throttling reduces dispatch rather than buying unusable VMs.

## Safe removal

Lifecycle: requested -> provisioning -> enrolling -> ready -> draining -> removable -> deleted;
failures become quarantined with an inspectable reason and retry/recovery state.

Begin drain by atomically excluding the worker from assignment. Wait for native work,
subtasks, review/handoff duties and active processes to reconcile. Check repositories for
dirty/untracked/unpushed work, save approved checkpoint data, receive coordinator custody
acknowledgment, and confirm no outstanding human-response return depends on that host.
Maintain Azure scale-in protection throughout this process. A drain timeout alerts and
retains the machine; it never authorizes killing a writer. Only a fresh fenced final check
can remove protection and delete the exact owned instance. Reconcile registration/credential
retirement only after provider deletion is confirmed. Spot eviction is outside version one.

## Messaging and human decisions

Keep a channel-independent event with origin host, qitem ID, request revision, question and
choice IDs, authorized human binding and immutable delivery ID. Hermes adapters map native
WhatsApp quoted IDs, Slack threads and Teams replies back to that event. Explicit choices,
custom answers and clarification requests produce typed native responses; language models
may help discuss a decision but may not invent approvals or retarget an answer.

Persist outbound messages and valid inbound responses before acknowledgment. Retry known
unsent work with backoff after an outage; revalidate revision, binding and question state
before sending. An uncertain platform submission is indeterminate and must not be blindly
reposted. Distinguish durable acceptance, platform submission, actual receipt, native answer
commit and manager continuation. Duplicate quotes/API retries produce one committed answer.
Retain questions across worker drain by routing them to durable coordinator-owned lineage.
For an indeterminate send, version one exposes operator reconciliation against platform
evidence; it remains unresolved until the operator confirms delivery or authorizes a new
attempt with an explicit duplicate-risk acknowledgment. Do not promise eventual delivery
without that resolution. Automatic platform-history reconciliation is a later adapter option.

The fleet continues independent work while a decision waits. Never default-approve spending,
access grants, merges or destructive actions merely to keep agents occupied. Durable pending
questions remain visible in the native interface and enabled messenger channel.

Current live proof: the latest genuine quoted WhatsApp reply parsed as sample=blue, but native
return admission refused unknown_destination_rig. Phone-to-plugin parsing is proven for this
attempt; answer commit/continuation remain unproven. Resolve the synthetic return route and
verify original-path readback before claiming messaging acceptance. Slack/Teams native tests
are separate from live configured-channel acceptance. WhatsApp is the required first live
channel for this deployment. Slack/Teams compatibility ships with deterministic native
adapter tests and explicit unverified-live status until recipients and environments exist.
Do not claim those channels live-proven from fixtures alone.

## Upstream contribution sequence

1. Propose the combined outcome and ask maintainers to choose acceptable core/extension seams.
2. Native human responses and durable relay: explicit choices/custom/clarification, outage and
   replay proof. Keep Hermes core generic hooks and native reply fixes in its own upstream PRs.
3. GitHub reconciliation and dispatch: eligibility, stable identities, reservations and
   progress/recovery receipts. Separate from provider infrastructure.
4. Generic elastic pool: leader/assignment fencing, enrollment policy, lifecycle, drain and
   dry-run CLI plan/status/pause/drain operations. Proposed commands are not currently shipped.
5. Optional Azure adapter/bootstrap/IaC, followed by optional Service Bus/Function transport.

One official feature proposal links the PR series. Do not bundle all infrastructure and
existing messaging changes into one unreviewable PR. Shipping the native core must not
require an Azure subscription, Hermes installation or a public inbound daemon listener.

## Acceptance evidence

Deterministic tests: missed/duplicate/out-of-order GitHub events; partial snapshots; two
schedulers racing; remote admission timeout; stale workers; delayed results; provider outage
and throttling; restart during provision/enroll/drain/delete; unknown resource ownership;
dirty/unpushed work; human waits and stale quoted replies; offline durable retry and duplicate
responses. Native package/build/typecheck/repository/workspace/UI gates remain required.

Bounded Azure proof after deployment configuration is approved: create one elastic instance,
verify pinned bootstrap/private admission, dispatch a synthetic repository task, observe
native pickup and independently verified output, keep permanent hosts working, and safely
drain/delete only the test instance. Demonstrate controller restart and a retained unsafe
drain; verify no resource leak and no duplicate ownership. Then enable automatic operation
within the selected ceiling. Resource HTTP health and submitted message IDs are insufficient.
Finally exercise automatic demand: a controlled eligible backlog must trigger expansion,
new workers must pick up distinct tasks, one genuine human wait must leave independent
work progressing, and verified idle surplus must trigger safe automatic contraction.
Require these observed policy transitions before claiming automatic scaling accepted.

Track eligible wait time, time to ready, real checkpoint freshness, accepted issue outcomes,
retries/recovery, pending human questions, blocked reasons and estimated compute cost.
Autoscaling is successful when more eligible work completes safely, not when more seats run.

## Decisions before live activation

The design recommends retaining all three permanent workers and initially capping seven
elastic workers at two issue slots each. The user must select the actual live ceiling and
spending envelope. Confirm provider headless authentication, repository scope and image;
resolve maintainer placement before upstream implementation. These do not block a dry-run
design or local deterministic planning, but they do block enabling cloud creation/enrollment.

## Primary references

- OpenRig `CONTRIBUTING.md`, `docs/reference/developing.md` and source seams above.
- Azure instance protection: https://learn.microsoft.com/en-us/azure/virtual-machine-scale-sets/virtual-machine-scale-sets-instance-protection
- Service Bus settlement: https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement
- Service Bus duplicate detection: https://learn.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection
