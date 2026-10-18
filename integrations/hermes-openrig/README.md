# OpenRig questions through Hermes native channels

This optional Hermes plugin lets the bound person discuss a native OpenRig question on WhatsApp, Slack or Teams, then send an explicit choice, custom answer, or clarification back to its original request. OpenRig's existing queue and human-response writer remain authoritative. A conversation or an LLM statement never records an approval.

The plugin uses Hermes' existing native platform adapters and supervised plugin task lifecycle. It creates no process, port, account, or credentials. Keep the plugin disabled until the actual WhatsApp profile, chat and sender have been verified. Telegram and OpenClaw are outside this integration. Each provider needs its own existing authorized configuration; this plugin never provisions a provider.

## Supported Hermes extension points

Verified against the deployed Hermes source `71d34bad1045610252eda02ee79549436896e784` (also compatible with `d923a5eb487a17c0a68801bba9ef0f56c81040a8`) and its [official plugin documentation](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins):

- `ctx.spawn_task` supervises polling and status notices. `ctx.on_unload` cancels them on unload; normal gateway shutdown also cancels them through its existing asyncio lifecycle.
- `register_platform_handler("whatsapp", factory)` supplies the adapter. `is_connected` and `send()` are public APIs; `SendResult.message_id` and `continuation_message_ids` provide platform receipt identities.
- `pre_gateway_dispatch` receives the native message and quoted-message identity. It runs before Hermes authorization, so this plugin independently checks the exact configured profile, chat and sender before handling an OpenRig reply.
- SDK71's owner-message prefix is recognized only with matching native owner metadata and message/chat/sender identities. Ordinary discussion retains the SDK marker; quoted commands still require the original question and bound sender.
- `ctx.get_config`, `ctx.profile_name` and durable `ctx.state` provide the existing plugin settings, authoritative profile and atomic profile-scoped state.

Platform receipt identities prove that the native adapter accepted a send and allow an eventual quoted reply to be associated with its question. They do not prove device delivery or that the person saw the message. In particular, the tested WhatsApp bridge returns a generated message ID without exposing handset delivery/read acknowledgments. Verify receipt in the actual conversation before treating the relay as working end to end; retain uncertain sends rather than blindly reposting them.

An invalid command on a recognized, bound question receives best-effort format guidance and a hashed question reference. It does not queue an answer. Validation notices use a bounded in-memory once-attempt cache; they are separate from the durable valid-response retry queue. Diagnostic logs contain failure codes, bounded counts and identity-presence booleans, without command text or message identities.

Use a compatible Hermes installation with its existing WhatsApp platform enabled. Install this directory as a trusted user plugin using Hermes' supported plugin installation procedure, then run `hermes plugins enable openrig-human-relay` in that gateway's existing profile and restart the gateway. This preserves other enabled plugins; the settings entry alone does not add this plugin to Hermes' `plugins.enabled` allow-list. Legacy pull needs no listener. Optional push uses the existing Hermes webhook adapter and a separately configured authenticated route; the plugin creates no listener and does not change Hermes access lists.

The WhatsApp connection factory starts one supervised poll task; the existing webhook platform, when enabled, also starts the same task so previously saved inbound replies can retry before WhatsApp connects. Neither factory creates a listener or changes routes. Reconnecting updates the adapter without starting another poller. While WhatsApp is disconnected, native questions and saved replies remain retained. If neither an existing webhook nor WhatsApp has connected since gateway startup, no factory has fired yet, so saved inbound retries wait for platform startup. On successful connection, the existing durable retry lanes resume.

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

WhatsApp may report the same person as a phone JID or a LID. Replies can match an existing binding through Hermes' native identity helper only when its saved mapping resolves exactly that two-identifier pair; unknown or conflicting mappings fail closed. Profile and channel must still match, and the quoted message must identify one question within the authorized chat/sender tuple. Slack and Teams keep exact identity matching. OpenRig receives the original bound tuple; any different observed WhatsApp tuple is retained only in the private reply audit.

In the existing Hermes plugin settings, set:

```yaml
plugins:
  entries:
    openrig-human-relay:
      settings:
        connector_id: hermes
        openrig_url: http://127.0.0.1:17433
        origin_hosts: [local, azure, azure2, azure3]
        bearer_file: /EXISTING/OPERATOR-MANAGED/OPENRIG-BEARER-FILE
        poll_seconds: 5
```

The URL must be the existing protected OpenRig endpoint reachable from that gateway (an existing private tunnel is suitable). The file must contain the already-authorized OpenRig bearer; alternatively `bearer_env` names an existing environment variable, defaulting to `OPENRIG_AUTH_BEARER_TOKEN`. Tokens are read internally and never stored in plugin receipts or placed on command lines. Remote host credentials remain in OpenRig's existing host registry and are resolved by the daemon, not exposed to WhatsApp or the LLM.

## Optional durable webhook delivery

Push requires Hermes native `plugin_hook` webhook support. SDK71 without that extension continues to support legacy pull. Configure an existing generic HMAC webhook route with `plugin_hook: openrig-human-relay` and the operator-managed secret in Hermes' normal route configuration. Pin that route with plugin setting `webhook_route: openrig-human-relay` (the default), and set `delivery_mode: webhook`. The native route dispatches exactly one async registered hook with `route_name`, canonical `profile` (`default` for the default profile), parsed `payload` and authenticated header `delivery_id`. Missing or duplicate hook registration fails closed in Hermes. Its existing HMAC authentication runs before this plugin.

Configure the matching OpenRig origin's `human-relay.json` with a `webhooks` entry containing `connectorId`, `profile`, optional `channel` (default `whatsapp`), `endpointUrl` for this existing route and `secretFile` naming the existing authorized shared HMAC secret. Hermes owns its route secret configuration; OpenRig reads its operator-managed secret file. OpenRig uses Standard Webhooks headers accepted by Hermes' generic route: `webhook-id` carries `deliveryId`, `webhook-timestamp` carries the Unix timestamp, and `webhook-signature` is `v1,` followed by the base64 HMAC-SHA256 of `<deliveryId>.<timestamp>.<exact UTF-8 JSON body>`. Use the same plain secret on both sides (not a `whsec_`-encoded secret). Hermes also supports its native generic V2 signature for other authorized senders. Do not put credentials in bindings, messages or LLM prompts. Webhook payloads cannot choose an endpoint or route.

The sole accepted event shape is:

```json
{
  "schema": "openrig.human-relay.v1",
  "type": "question.created",
  "deliveryId": "ORIGINAL_DELIVERY_ID",
  "fingerprint": "SHA256_HEX_OF_CANONICAL_DELIVERY",
  "delivery": {"...": "existing native HumanRelayDelivery"}
}
```

Canonical delivery JSON recursively sorts object keys, uses compact separators, preserves Unicode and is encoded as UTF-8. Admission verifies that fingerprint, the original delivery identity, connector and profile. The header ID must equal the envelope and delivery IDs. An atomic `ctx.state` write precedes HTTP 200. The exact acceptance body is `{"schema":"openrig.human-relay.v1","status":"accepted","deliveryId":"...","fingerprint":"..."}`. This confirms durable admission only. Existing platform receipt HTTP remains the separate proof that a provider submitted a message.

Duplicate `(originHostId, deliveryId)` callbacks return that same stored acknowledgment, including after a gateway restart or lost HTTP response. A changed payload with the same identity returns 409; malformed events return 400; wrong route/profile returns 403; unavailable durable storage returns 503. Keep the original attempt and payload unchanged across HTTP retries. Stored webhook acknowledgment records remain retained for stable retry results.

Disconnected native adapters leave admissions queued. The existing supervised poller drains queued work, saved platform receipts and incoming answers. `delivery_mode: webhook` disables only legacy pull. Immediately before each platform send, the plugin asks the original OpenRig origin to validate the exact delivery, attempt, question revision and current binding. Unavailable or malformed validation holds it for retry; a changed or answered request suppresses the platform send. The plugin persists `sending` before provider I/O. Restarted or timed-out sends with no saved platform receipt remain indeterminate and never automatically repost.

## Optional outbound-only local bridge

When OpenRig is reachable from Hermes through an existing private connection, but Hermes' existing webhook listener is bound to loopback, use `delivery_mode: webhook-bridge`. Configure the origin's webhook target with `mode: pull-bridge` so its daemon retains immutable webhook events for this bridge instead of pushing them. Keep the binding and matching target connector, profile, channel and endpoint unchanged while an event is pending.

In the existing Hermes plugin settings:

```yaml
delivery_mode: webhook-bridge
bridge_url: http://127.0.0.1:8644/webhooks/openrig-human-relay
bridge_secret_file: /EXISTING/LOCAL/HERMES-ROUTE-SECRET
bridge_channels: [whatsapp]
origin_hosts: [local, azure, azure2, azure3]
```

Use the actual existing local route path and port; this example creates no listener. `bridge_url` must be a numeric loopback HTTP URL, without embedded credentials or a fragment. Redirects and ambient proxies are disabled. `bridge_secret_file` names the already-authorized plain HMAC secret of that route. The payload cannot nominate another destination. This mode needs no inbound network expansion and uses no LLM.

After existing receipt, response and queued-delivery retries, each poll rotates through one configured origin/channel pair (at most ten origins and three distinct supported channels). It requests at most two immutable events from native `webhook-pull`, then signs and POSTs each canonical event to the configured loopback route with a five-second bound. The normal native webhook hook validates and durably admits the event even when the platform is disconnected. Only an exact schema/status/delivery ID/fingerprint acceptance acknowledgment permits native `webhook-accepted`; that acknowledgment carries the original origin, attempt and complete channel/profile/chat/sender binding. The daemon separately retains platform submission receipts.

A lost local HTTP or native acceptance acknowledgment repeats the original event. The durable receiver returns the same stored acknowledgment and never resends a platform message just because a bridge acknowledgment was lost. Local storage refusal, incorrect acknowledgment or redirect leaves the native event pending. The existing queue and send uncertainty rules continue to apply.

## Channel binding and provider limits

Set optional `channel` on the native binding to `slack` or `teams`; omitted `channel` preserves the existing WhatsApp binding and reply identity. The plugin attaches each through `register_platform_handler` and chooses an adapter by that native channel. A quoted reply must match the saved channel, profile, chat, sender and actual provider receipt. Non-WhatsApp response payloads carry `channel`; their inbound deduplication keys include that channel. Text cannot supply the actor or quoted identity. SDK71 WhatsApp stanza-only quotes and identity-matched owner prefixes retain their original behavior.

Slack's native `reply_to_message_id` is its thread root. Teams requires a native adapter that preserves activity `reply_to_id` as `MessageEvent.reply_to_message_id`; an older adapter that drops it can submit questions but cannot authorize quoted responses. The plugin does not infer a quote from conversation text. Unconfigured or disconnected providers remain queued. These adapters do not expose a caller-chosen send idempotency key or reliable query-by-key reconciliation through this integration, so an uncertain provider attempt requires reconciliation. Provider IDs prove submission, not human reading.

## On the phone

Reply to the actual question message to discuss its context and alternatives with Hermes. Only a matching bound sender/profile/chat receives current native context. If OpenRig is unavailable, Hermes receives an explicitly labeled historical snapshot. This read-only enrichment preserves the native event identity and never sends an answer.

To submit, reply to that same question message with:

- `/choose 1 2` selects option 2 of question 1. For one question, `/choose 2` also works.
- `/answer Your explicit answer` sends native `kind: custom` text. For multiple questions this fills only the first unanswered slot; it does not approve every question.
- `/ask Your clarification` sends native `kind: clarification`. It leaves the decision pending.

Hermes sends truthful status notices for a durably queued reply, a confirmed native answer or clarification, and a native refusal. These notices are best-effort once attempted; they do not prove handset delivery. The durable native receipt and saved response remain authoritative.

The original origin host, qitem ID and request revision are preserved. Native inbound message identity produces the reply deduplication key. Malformed or mismatched replies to a known OpenRig question are consumed without entering the LLM approval path. Unrelated chat remains ordinary Hermes conversation. This version has no conversational approval tool: the verified generic tool context does not expose a trustworthy quoted-message sender binding, so a model cannot turn “I think so” into approval.

## Offline delivery and honest receipts

Native pending questions remain in the existing queue while Hermes or a configured platform is offline. A connected plugin pulls bounded pages from explicit origins. Before any platform send, OpenRig persists a sending claim in its existing `outbox_entries` under `human-relay-`; Hermes also persists its local sending claim. This namespace cannot execute as a seat wake. A definite not-attempted delivery can retry with backoff; one held question does not hide later questions.

A platform message ID is the evidence of a platform submission, not proof that a handset displayed or a person read it. Hermes saves actual platform IDs before acknowledging them to OpenRig. A lost acknowledgment retries only that receipt, never the send. The current WhatsApp adapter has no caller-supplied send idempotency key or query-by-key API. A crash or timeout after an attempt without a saved receipt remains **indeterminate** and requires reconciliation; automatic reposting would risk duplicates. The same conservative rule applies to a crash between claiming and sending when the boundary cannot be proven.

Incoming explicit responses are atomically saved in `ctx.state` **before** HTTP. When OpenRig is offline, polling retries the same immutable response tuple. A committed native answer with a lost HTTP acknowledgment returns the same native receipt on retry. Terminal stale/conflict/auth failures remain visible in plugin state; they never retarget a newer question. Stale outbound messages cannot authorize an answer because the native writer validates current request state and revision again.

Plugin state retains all unresolved outgoing deliveries and pending incoming replies. Confirmed terminal caches can be pruned after 30 days; native queue/outbox audit remains. A storage/quota refusal never counts as successful queuing. Native unanswered requests survive that refusal and require operator reconciliation rather than an invented success report.

## Verification

```sh
npm exec -- vitest run packages/daemon/test/human-relay.test.ts --maxWorkers=1
python3 -m unittest discover -s integrations/hermes-openrig/tests
```

These are hermetic SQLite/API and fake-adapter tests. They do not contact WhatsApp, create credentials or prove a real phone delivery. A live test requires the verified binding and a clearly synthetic question; preserve the distinction between synthetic interface proof and a genuine human decision.

### OpenRig sender configuration

Keep recipient identity in `human-relay.json` version 1 `bindings`. Add a separate optional `webhooks` array; transport settings never enter legacy WhatsApp delivery or response hashes:

```json
{"version":1,"bindings":[{"connectorId":"hermes","humanSession":"human@kernel","profile":"default","chatId":"VERIFIED_CHAT","senderId":"VERIFIED_SENDER"}],"webhooks":[{"connectorId":"hermes","profile":"default","channel":"whatsapp","mode":"pull-bridge","endpointUrl":"http://127.0.0.1:8644/webhooks/openrig-human-relay"}]}
```

`pull-bridge` is for an outbound-only Hermes installation with an existing loopback listener. It excludes the target from OpenRig's HTTP push scheduler. The authenticated `webhook-pull` route leases the same immutable native outbox event; the local bridge passes it through the real HMAC-authenticated Hermes webhook and confirms its exact durable acknowledgment through `webhook-accepted`. The platform receipt remains separate. This proves a polling bridge into a local webhook, not remote inbound reachability.

For directly reachable trusted receivers use `mode: "push"` (default) and a `secretFile` readable by that OpenRig daemon, containing the configured plain route secret. The deterministic scheduler claims at most two events per target/pass, signs Standard Webhooks headers, disables redirects, and accepts only the matching `openrig.human-relay.v1` acceptance receipt. Endpoint or mode changes hold outstanding webhook attempts and prevent an already accepted offline receiver from sending. Uncertain HTTP retries keep the original attempt and fingerprint; uncertain platform sends require reconciliation.

All relay operations require the existing mission-control bearer. Remote origins resolve only through OpenRig's server-side host registry. Payloads cannot nominate callback URLs or a human actor. `/validate` confirms the current recipient, attempt, transport, pending state and request revision without inventing a platform message receipt. `/respond` continues to use the native human-response writer.
