# Durable human-relay webhooks

Goal: deliver OpenRig questions through Hermes on Proxmox, retaining original request identity and verified quoted replies across WhatsApp, Slack and Teams. Continue independent agent work while a decision awaits its human.

Architecture: the existing native outbox claims deliveries, an opt-in deterministic scheduler signs HTTP events to an operator-configured Hermes webhook route, and an owning-profile plugin persists each event before acknowledging. Platform receipts remain separate. Replies use the native OpenRig response writer. Hermes core gains only a generic singleton plugin hook; channel behavior lives in the OpenRig plugin.

Tech stack: existing SQLite outbox, TypeScript/Hono/PollScheduler, Python Hermes plugin state and native adapters, existing aiohttp webhook authentication. No model call is required for transport, validation, retries or explicit answers.

## Global constraints

Preserve legacy WhatsApp delivery/reply IDs. Transport endpoint and secrets are not recipient identity. Immutable attempt/payload survive HTTP uncertainty. Never resend uncertain platform sends. Revalidate request state/revision/binding before draining offline messages. No payload-selected destination, callback URL or human actor. Keep private listener/access boundaries and existing ownership/acceptance gates. Slack/Teams live delivery requires configured verified recipients; fixture proof is not live proof.

## Tasks

1. Refresh upstream and preserve both migration/fixture changes. Write failing daemon tests for legacy identity, webhook claim/retry/restart, acceptance distinction and pre-send stale validation; implement optional transport and native validate route.
2. Write failing plugin tests for persistent webhook acceptance, immutable payload conflicts, offline replay and channel-bound replies; implement on existing plugin lifecycle/state/native adapters. Keep legacy pull as default.
3. Write failing Hermes native route tests; implement generic authenticated singleton async plugin hook, fail closed and bypass agent/transient dedup. Preserve native Teams reply metadata. Use canonical Hermes test runner.
4. Run build/typecheck/affected suites, review with Astra, fix material findings. Deploy a minimal reviewed Hermes patch and plugin through existing private Proxmox administration, preserve state/config, and prove authentic webhook/receipt/reply behavior with synthetic data.
5. Verify all Azure origins, publish reviewable PRs with exact tests and explicit remaining proof limits.

## Review focus

Lost HTTP acknowledgments, receiver restart, storage failures, stale offline questions, duplicate response writes, crash between platform send and receipt, cross-profile/sender rejection, existing WhatsApp identity compatibility. Native receipt, productive agent work and human acceptance are distinct evidence.
