# OpenRig questions in Hermes WhatsApp

This optional Hermes plugin lets the bound person discuss a native OpenRig question on WhatsApp, then send an explicit choice, custom answer, or clarification back to its original request. OpenRig's existing queue and human-response writer remain authoritative. A conversation or an LLM statement never records an approval.

The plugin uses Hermes' existing WhatsApp adapter and existing API-server lifecycle. It creates no process, port, account, or credentials. Keep the plugin disabled until the actual WhatsApp profile, chat and sender have been verified. Telegram and OpenClaw are outside this first slice.

## Supported Hermes extension points

Verified against Hermes source `d923a5eb487a17c0a68801bba9ef0f56c81040a8` and its [official plugin documentation](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins):

- `register_platform_handler("api_server", factory)` supplies the existing aiohttp application before `AppRunner.setup()`. Its `cleanup_ctx` starts and cancels polling with that gateway.
- `register_platform_handler("whatsapp", factory)` supplies the adapter. `is_connected` and `send()` are public APIs; `SendResult.message_id` and `continuation_message_ids` provide platform receipt identities.
- `pre_gateway_dispatch` receives the native message and quoted-message identity. It runs before Hermes authorization, so this plugin independently checks the exact configured profile, chat and sender before handling an OpenRig reply.
- `ctx.get_config`, `ctx.profile_name` and durable `ctx.state` provide the existing plugin settings, authoritative profile and atomic profile-scoped state.

Use a compatible Hermes installation with both existing platforms enabled. Install this directory as a trusted user plugin using Hermes' supported plugin installation procedure, then restart that gateway so the API-server startup/shutdown hook is installed. This plugin does not enable either platform or change Hermes access lists. A gateway without the existing API server has no polling lifecycle and cannot provide offline delivery.

## Explicit binding

On each participating OpenRig origin, provide an operator-managed `~/.openrig/human-relay.json`:

```json
{
  "version": 1,
  "bindings": [{
    "connectorId": "hermes",
    "humanSession": "human@kernel",
    "profile": "VERIFIED_HERMES_PROFILE",
    "chatId": "VERIFIED_NATIVE_WHATSAPP_CHAT_ID",
    "senderId": "VERIFIED_NATIVE_WHATSAPP_SENDER_ID"
  }]
}
```

These are placeholders, not example credentials or an inferred phone number. Use identities observed from the actual authorized native WhatsApp event. The initial binding is the existing local `human@kernel`; this does not register an external human or relax Slack's registry rules. Missing configuration is inert. Configure only the intended origin hosts.

In the existing Hermes plugin settings, set:

```yaml
plugins:
  entries:
    openrig-human-relay:
      enabled: true
      settings:
        connector_id: hermes
        openrig_url: http://127.0.0.1:17433
        origin_hosts: [local, azure, azure2, azure3]
        bearer_file: /EXISTING/OPERATOR-MANAGED/OPENRIG-BEARER-FILE
        poll_seconds: 5
```

The URL must be the existing protected OpenRig endpoint reachable from that gateway (an existing private tunnel is suitable). The file must contain the already-authorized OpenRig bearer; alternatively `bearer_env` names an existing environment variable, defaulting to `OPENRIG_AUTH_BEARER_TOKEN`. Tokens are read internally and never stored in plugin receipts or placed on command lines. Remote host credentials remain in OpenRig's existing host registry and are resolved by the daemon, not exposed to WhatsApp or the LLM.

## On the phone

Reply to the actual question message to discuss its context and alternatives with Hermes. Only a matching bound sender/profile/chat receives current native context. If OpenRig is unavailable, Hermes receives an explicitly labeled historical snapshot. This read-only enrichment preserves the native event identity and never sends an answer.

To submit, reply to that same question message with:

- `/choose 1 2` selects option 2 of question 1. For one question, `/choose 2` also works.
- `/answer Your explicit answer` sends native `kind: custom` text. For multiple questions this fills only the first unanswered slot; it does not approve every question.
- `/ask Your clarification` sends native `kind: clarification`. It leaves the decision pending.

Hermes sends truthful status notices for a durably queued reply, a confirmed native answer or clarification, and a native refusal. These notices are best-effort once attempted; they do not prove handset delivery. The durable native receipt and saved response remain authoritative.

The original origin host, qitem ID and request revision are preserved. Native inbound message identity produces the reply deduplication key. Malformed or mismatched replies to a known OpenRig question are consumed without entering the LLM approval path. Unrelated chat remains ordinary Hermes conversation. This version has no conversational approval tool: the verified generic tool context does not expose a trustworthy quoted-message sender binding, so a model cannot turn “I think so” into approval.

## Offline delivery and honest receipts

Native pending questions remain in the existing queue while Hermes or WhatsApp is offline. A connected plugin pulls bounded pages from explicit origins. Before any platform send, OpenRig persists a sending claim in its existing `outbox_entries` under `human-relay-`; Hermes also persists its local sending claim. This namespace cannot execute as a seat wake. A definite not-attempted delivery can retry with backoff; one held question does not hide later questions.

A platform message ID is the evidence of a platform submission, not proof that a handset displayed or a person read it. Hermes saves actual platform IDs before acknowledging them to OpenRig. A lost acknowledgment retries only that receipt, never the send. The current WhatsApp adapter has no caller-supplied send idempotency key or query-by-key API. A crash or timeout after an attempt without a saved receipt remains **indeterminate** and requires reconciliation; automatic reposting would risk duplicates. The same conservative rule applies to a crash between claiming and sending when the boundary cannot be proven.

Incoming explicit responses are atomically saved in `ctx.state` **before** HTTP. When OpenRig is offline, polling retries the same immutable response tuple. A committed native answer with a lost HTTP acknowledgment returns the same native receipt on retry. Terminal stale/conflict/auth failures remain visible in plugin state; they never retarget a newer question. Stale outbound messages cannot authorize an answer because the native writer validates current request state and revision again.

Plugin state retains all unresolved outgoing deliveries and pending incoming replies. Confirmed terminal caches can be pruned after 30 days; native queue/outbox audit remains. A storage/quota refusal never counts as successful queuing. Native unanswered requests survive that refusal and require operator reconciliation rather than an invented success report.

## Verification

```sh
npm exec -- vitest run packages/daemon/test/human-relay.test.ts --maxWorkers=1
python3 -m unittest discover -s integrations/hermes-openrig/tests
```

These are hermetic SQLite/API and fake-adapter tests. They do not contact WhatsApp, create credentials or prove a real phone delivery. A live test requires the verified binding and a clearly synthetic question; preserve the distinction between synthetic interface proof and a genuine human decision.
