# Manager recovery of stalled work

Use this procedure when workers have actionable work but are not producing new results. HTTP health, a running process, a successful wake, and a new session are not proof of task progress.

The manager owns diagnosis, bounded repair, and verification. OpenRig's existing watchdog schedules inspection; the stuck-queue sweep and native recovery rows preserve ownership and deduplicate notices. Do not add a second work queue or a restart on every timer tick.

## Inspect before acting

Read the current native task, its owner and handoffs, the seat's current occupant generation, and recent tool/provider results. Use the installed `rig ps`, `rig capture`, `rig queue`, `rig seat`, and `rig watchdog` help to discover supported operations. Private installations may have additional input receipts; capability-check them before use. Never substitute terminal keystrokes when a native operation refuses a control command.

Use the same native recovery row for the same unresolved incident. Record the seat generation, affected task IDs, exact failure and evidence reference, last successful work, attempted action, outcome, and next verification. A pending or claimed recovery row has one recovery owner. A stale generation or a completed incident cannot authorize repair of its replacement.

| Observed blocker | Manager action | Required proof |
| --- | --- | --- |
| Compaction failed or was cancelled | Preserve scope, queue custody, branches, and a durable recap. Inspect the failure. If the conversation cannot continue, plan one supported handover using that carrier. | Actual successful compaction or a replacement that reads the carrier and resumes the original authorized task. |
| Provider rejects message history with HTTP 400 | Identify the rejected payload/history field; do not retry the same invalid request. Repair owned configuration or plan one conversation rebuild when required. | A real accepted provider turn and task tool/result after the repair. |
| Output allowance is below the provider's minimum | Verify the effective model/profile and request limits. Correct the owned limit or select an already authorized compatible model using the audited seat operation. | Effective request values and a successful real response; a model label alone is insufficient. |
| Commit is unpushed or a handoff is missing | Reconcile live branches, current writer, checks, review, and receiving custody. Complete the existing authorized delivery path; do not restart the worker to fix a Git handoff. | Published commit/PR or an acknowledged native successor, with the required downstream gates intact. |
| Activity is stale or unknown | Reconcile native state, process identity, current generation, and recent tool/result evidence. Refresh supported observations before choosing a lifecycle action. | Fresh, consistent evidence. Unknown activity never proves idle or authorizes interruption. |

## Bound the recovery

Make at most one lifecycle attempt for an unchanged incident. Read the plan before execution. On timeout, inspect that original attempt and the actual occupant before retrying; an uncertain outcome remains uncertain. A later action requires new evidence and a recorded manager disposition, not another timer tick.

Before replacing a conversation, prepare an explicit carrier containing current authorized task IDs, working directory/branch, exact next action, already observed side effects, and all held tasks and prohibitions. Validate the configured rebuild artifact paths, their readability and exact bytes before execution; writing a recap file alone does not prove it will be delivered. Some installed handover plans do not list selected artifacts: record that limitation instead of treating a successful dry-run as carrier proof. Check the execution result's delivered artifacts and verify that the successor reads the intended scope before taking task actions. Missing recap, missing provenance, or a fresh prompt must not be called transferred continuity. Do not let old checkpoints or unrelated blocked rows become new work by accident.

Preserve task source, destination, body, owner, and existing branch/worktree reservations. Do not clear a human, credential, acceptance, review, spending, or production hold merely to make recovery appear successful. Ask the person through a native linked human question when a real decision is missing; keep independent eligible work moving within the actual physical writer and heavy-test limits.

If repair cannot proceed, record the exact unavailable capability or decision and its evidence on the incident. Continue other eligible work. Stop repeated identical retries and duplicate prompts.

## Verify and close

Re-read the same task and current occupant after the action. Require renewed real provider/tool progress and a durable result or handoff. Record failed attempts as well as later successes; retries do not erase failures. Close only the recovery incident whose repair was proved. Product completion, independent review, release, and acceptance retain their own gates.

Configure one existing watchdog targeting the manager for inspection, with separate scan and wake cadences. Reuse the existing stuck-sweep ownership and recovery deduplication. Prefer no-op while the relevant owner is making progress; a reminder is not a new top-priority assignment or authority to interrupt productive commands.
