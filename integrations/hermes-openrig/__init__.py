"""Native OpenRig questions through existing Hermes native platform adapters.
No LLM turn can manufacture a native answer. Quoted explicit commands own submission.
"""
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import pathlib
import time
import urllib.error
import urllib.request


def _same_bound_identity(observed, bound, channel):
    if observed == bound:
        return True
    if channel != 'whatsapp':
        return False
    try:
        from gateway.whatsapp_identity import (canonical_whatsapp_identifier, expand_whatsapp_aliases,
                                               normalize_whatsapp_mention_jid)
        observed_jid = normalize_whatsapp_mention_jid(observed)
        bound_jid = normalize_whatsapp_mention_jid(bound)
        if not observed_jid or not bound_jid:
            return False
        observed_number, _, observed_domain = observed_jid.partition('@')
        bound_number, _, bound_domain = bound_jid.partition('@')
        if observed_jid == bound_jid:
            return True
        # Only a single native PN↔LID pair can extend an existing binding.
        # Domain stripping alone or a conflicting transitive map cannot authorize it.
        if observed_domain == bound_domain or observed_number == bound_number:
            return False
        pair = {observed_number, bound_number}
        if expand_whatsapp_aliases(observed_jid) | expand_whatsapp_aliases(bound_jid) != pair:
            return False
        canonical = canonical_whatsapp_identifier(observed_jid)
        return bool(canonical) and canonical == canonical_whatsapp_identifier(bound_jid)
    except Exception:
        return False  # Missing/unreadable native identity support never broadens a binding.


def _binding_matches(delivery, binding, channel):
    profile, chat, sender = binding
    return {'profile_matches': profile == delivery['profile'],
            'chat_matches': _same_bound_identity(chat, delivery['chatId'], channel),
            'sender_matches': _same_bound_identity(sender, delivery['senderId'], channel)}


class RelayFailure(Exception):
    def __init__(self, status, reason):
        super().__init__(reason)
        self.status = status


class NativeTransport:
    def __init__(self, ctx):
        self.ctx = ctx

    async def post(self, operation, payload):
        def request():
            base = self.ctx.get_config('openrig_url', '').rstrip('/')
            if not base.startswith(('http://', 'https://')):
                raise ValueError('Configure the existing trusted OpenRig URL.')
            token = os.environ.get(self.ctx.get_config('bearer_env', 'OPENRIG_AUTH_BEARER_TOKEN'), '')
            token_file = self.ctx.get_config('bearer_file', '')
            if not token and token_file:
                token = pathlib.Path(token_file).expanduser().read_text().strip()
            headers = {'Content-Type': 'application/json'}
            if token:
                headers['Authorization'] = 'Bearer ' + token
            req = urllib.request.Request(base + '/api/mission-control/human-relay/' + operation,
                                         data=json.dumps(payload).encode(), headers=headers, method='POST')
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                # Never include credentials or request headers in state/logging.
                try:
                    reason = json.loads(error.read()).get('error', 'native_request_refused')
                except (ValueError, AttributeError):
                    reason = 'native_request_refused'
                raise RelayFailure(error.code, str(reason)) from None
        return await asyncio.to_thread(request)


def render_question(delivery):
    lines = [delivery.get('summary') or 'OpenRig needs your input.', delivery['body']]
    for qi, question in enumerate(delivery['questions'], 1):
        lines.append(f"{qi}. {question['question']}")
        for oi, option in enumerate(question['options'], 1):
            mark = ' (recommended)' if option.get('recommended') else ''
            lines.append(f"  {oi}. {option['label']}{mark}")
            if option.get('consequences'):
                lines.append('     ' + option['consequences'])
    if delivery.get('evidenceRef'):
        lines.append('Context: ' + delivery['evidenceRef'])
    lines.append('You can discuss this question with Hermes by replying to this message. '
                 'To send an answer to OpenRig, reply with /choose <question number> <option number>, '
                 '/answer <your answer>, or /ask <your clarification>. For one question, /choose <option number> also works. '
                 'Conversation alone does not record an answer.')
    return '\n\n'.join(lines)


class RelayPlugin:
    def __init__(self, ctx, transport=None, now=None):
        self.ctx = ctx
        self.now = now or time.time
        self.transport = transport or NativeTransport(ctx)
        self.adapter = None
        self.adapters = {}
        self.connector = ctx.get_config('connector_id', 'hermes')
        self.profile = ctx.profile_name or 'default'
        self.delivery_mode = ctx.get_config('delivery_mode', 'pull')
        self.webhook_route = ctx.get_config('webhook_route', 'openrig-human-relay')
        self.webhook_lock = asyncio.Lock()
        self.hosts = ctx.get_config('origin_hosts', ['local'])[:10]
        self.bridge = None
        self.bridge_cursor = 0
        if self.delivery_mode == 'webhook-bridge':
            # Native plugin loaders may load this file without a package namespace.
            spec = importlib.util.spec_from_file_location('openrig_local_bridge', pathlib.Path(__file__).with_name('_bridge.py'))
            bridge_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(bridge_module)
            self.bridge = bridge_module.LocalWebhookBridge(ctx)
            if not isinstance(self.hosts, list) or not self.hosts or any(not isinstance(h, str) or not h for h in self.hosts):
                raise ValueError('The local webhook bridge needs explicit origin host aliases.')
        self.poll_lock = asyncio.Lock()
        self.inbound_lock = asyncio.Lock()
        self.host_cursor = 0
        self.notice_tasks = {}
        self.notice_limit = asyncio.Semaphore(2)
        self.poll_task = None
        self.stopped = False
        ctx.on_unload(self.stop)

    def attach_channel(self, channel, native, adapter):
        self.adapters[channel] = adapter
        if channel == 'whatsapp':
            self.adapter = adapter  # Preserve the original public WhatsApp attachment.
        self.start()

    def attach_whatsapp(self, native, adapter):
        self.attach_channel('whatsapp', native, adapter)

    def platform_adapter(self, channel):
        return self.adapter if channel == 'whatsapp' else self.adapters.get(channel)

    def outbound_key(self, item):
        return item.get('storageKey', item['delivery']['deliveryId'])

    def outgoing_reply(self, payload):
        return next((item for item in self.ctx.state.get('outbound', {}).values()
                     if item['delivery']['deliveryId'] == payload['deliveryId']
                     and item['hostId'] == payload['hostId']
                     and item['delivery'].get('channel', 'whatsapp') == payload.get('channel', 'whatsapp')), None)

    async def handle_webhook(self, *, route_name, profile, payload, delivery_id, **kwargs):
        # Hermes has already authenticated the configured native HMAC route. Payload
        # content cannot nominate routes, profiles, transports, or callback handlers.
        if route_name != self.webhook_route or (profile or 'default') != self.profile:
            return {'status': 403, 'body': {'error': 'relay_route_or_profile_mismatch'}}
        try:
            if (not isinstance(payload, dict) or set(payload) != {'schema', 'type', 'deliveryId', 'fingerprint', 'delivery'}
                    or payload['schema'] != 'openrig.human-relay.v1' or payload['type'] != 'question.created'):
                raise ValueError('invalid_envelope')
            d = payload['delivery']
            required = ('deliveryId', 'attemptId', 'originHostId', 'qitemId', 'requestRevision', 'connectorId',
                        'humanSession', 'profile', 'chatId', 'senderId', 'body')
            if (not isinstance(d, dict) or any(not isinstance(d.get(k), str) or not d[k] for k in required)
                    or d.get('channel', 'whatsapp') not in ('whatsapp', 'slack', 'teams')
                    or not isinstance(d.get('questions'), list)
                    or d['connectorId'] != self.connector or d['profile'] != self.profile
                    or d['deliveryId'] != payload['deliveryId'] or delivery_id != payload['deliveryId']):
                raise ValueError('invalid_delivery')
            render_question(d)  # Reject malformed native question shapes before acknowledging storage.
            canonical = json.dumps(d, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
            fingerprint = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
            if payload['fingerprint'] != fingerprint:
                raise ValueError('invalid_fingerprint')
            identity = 'webhook:' + hashlib.sha256(json.dumps([d['originHostId'], d['deliveryId']],
                                               ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
            ack = {'schema': 'openrig.human-relay.v1', 'status': 'accepted',
                   'deliveryId': d['deliveryId'], 'fingerprint': fingerprint}
        except (ValueError, TypeError, KeyError, AttributeError):
            return {'status': 400, 'body': {'error': 'invalid_relay_event'}}
        try:
            async with self.webhook_lock:
                existing = self.ctx.state.get('outbound', {}).get(identity)
                if existing:
                    if existing.get('fingerprint') != fingerprint:
                        return {'status': 409, 'body': {'error': 'delivery_id_payload_conflict'}}
                    return {'status': 200, 'body': existing['ack']}
                item = {'storageKey': identity, 'delivery': json.loads(canonical), 'hostId': d['originHostId'],
                        'state': 'queued', 'at': self.now(), 'fingerprint': fingerprint, 'ack': ack}
                self.save_outbound(identity, item)  # Atomic durable admission before HTTP acceptance.
            return {'status': 200, 'body': ack}
        except Exception:
            return {'status': 503, 'body': {'error': 'relay_storage_unavailable'}}

    def attach_webhook(self, native, adapter):
        # Existing webhook calls its factory with None after binding; only start
        # supervised retries, without inspecting its app or changing routes.
        self.start()

    def start(self):
        if not self.stopped and (self.poll_task is None or self.poll_task.done()):
            self.poll_task = self.ctx.spawn_task(self.run(), name='openrig-human-relay:poll')

    def stop(self):
        self.stopped = True
        for task in [self.poll_task, *self.notice_tasks.values()]:
            if task and not task.done() and not task.get_loop().is_closed():
                task.get_loop().call_soon_threadsafe(task.cancel)

    async def run(self):
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).warning("OpenRig relay poll failed; native questions and saved responses remain retained.")
                # A storage/HTTP failure never authorizes a send or discards a native question.
                pass
            await asyncio.sleep(max(2, self.ctx.get_config('poll_seconds', 5)))

    def save_outbound(self, identity, value):
        saved = self.ctx.state.get('outbound', {})
        saved[identity] = value
        self.ctx.state.set('outbound', saved)

    def save_inbound(self, identity, value):
        saved = self.ctx.state.get('inbound', {})
        # Notification completion must never overwrite the authoritative response
        # state or be lost when an HTTP attempt finishes concurrently.
        for field in ('noticeAttemptedState', 'noticeMessageId'):
            if field not in value and field in saved.get(identity, {}):
                value[field] = saved[identity][field]
        saved[identity] = value
        self.ctx.state.set('inbound', saved)

    def notice_text(self, item):
        if item['state'] == 'recorded':
            if item['payload']['response']['kind'] == 'clarification':
                return 'OpenRig recorded your clarification request. The decision is still pending.'
            if not item['receipt']['complete']:
                return 'OpenRig recorded this part of your answer. Other questions are still waiting.'
            return 'OpenRig recorded your answer.'
        if item['state'] == 'refused':
            if item.get('error') == 'stale_human_request':
                return 'OpenRig did not accept this reply because the question changed or is no longer pending. Your original reply is retained for review. Please request the current question.'
            return 'OpenRig did not accept this reply. It is retained for review, and no newer question was substituted.'
        return 'Your reply is saved in Hermes and queued for confirmation by OpenRig. A native receipt has not been confirmed yet.'

    def schedule_notice(self, identity, failure=None):
        if self.stopped or (identity in self.notice_tasks and not self.notice_tasks[identity].done()):
            return
        async def deliver():
            async with self.notice_limit:
                try:
                    item = self.ctx.state.get('inbound', {}).get(identity)
                    channel = item['payload'].get('channel', 'whatsapp') if item else (failure[2] if failure else 'whatsapp')
                    adapter = self.platform_adapter(channel)
                    if not adapter or not adapter.is_connected:
                        return
                    if failure:
                        chat, message_id = failure[:2]
                        await adapter.send(chat, 'Hermes could not save your reply, so it was not queued. Please retry after the storage issue is resolved.', reply_to=message_id)
                        return
                    item = self.ctx.state.get('inbound', {}).get(identity)
                    if not item or item.get('noticeAttemptedState') == item['state']:
                        return
                    phase = item['state']
                    item['noticeAttemptedState'] = phase
                    self.save_inbound(identity, item)  # Persist before sending a status notice.
                    result = await adapter.send(item['payload']['chatId'], self.notice_text(item), reply_to=item['payload']['messageId'])
                    # Reload after network I/O: a native answer may have committed while
                    # this best-effort status notice was in flight.
                    current = self.ctx.state.get('inbound', {}).get(identity)
                    if current and getattr(result, 'message_id', None):
                        current['noticeMessageId'] = result.message_id
                        self.save_inbound(identity, current)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logging.getLogger(__name__).warning('OpenRig reply status notification could not be confirmed; durable response state is unchanged.')
        task = self.ctx.spawn_task(deliver(), name='openrig-human-relay:notice')
        self.notice_tasks[identity] = task
        def finished(done):
            if self.notice_tasks.get(identity) is done:
                self.notice_tasks.pop(identity, None)
        task.add_done_callback(finished)

    async def ack_platform(self, item):
        d = item['delivery']
        outcome = 'not-attempted' if item['state'] == 'not-attempted-pending' else ('platform-sent' if item.get('messageIds') else 'unknown')
        payload = {**d, 'hostId': item['hostId'], 'outcome': outcome}
        if item.get('messageIds'):
            payload['messageIds'] = item['messageIds']
        result = await self.transport.post('receipt', payload)
        if item.get('messageIds') and result.get('state') != 'delivered':
            raise ValueError('Native platform receipt was not acknowledged.')
        if outcome == 'not-attempted':
            if result.get('state') != 'pending':
                raise ValueError('Native definite-unsent outcome was not acknowledged.')
            item['state'] = 'not-attempted'
        else:
            item['state'] = 'acknowledged' if item.get('messageIds') else 'indeterminate'
        self.save_outbound(self.outbound_key(item), item)

    async def flush_platform_receipt(self, item):
        try:
            await self.ack_platform(item)
        except asyncio.CancelledError:
            raise
        except RelayFailure as error:
            item['error'] = str(error)
            if error.status in (400, 401, 403, 404, 409):
                item['state'] = 'receipt-refused'
            else:
                self.defer(item)
            self.save_outbound(self.outbound_key(item), item)
        except Exception as error:
            item['error'] = type(error).__name__
            self.defer(item)
            self.save_outbound(self.outbound_key(item), item)

    def defer(self, item):
        item['attempts'] = item.get('attempts', 0) + 1
        item['nextAttemptAt'] = self.now() + min(5 * 2 ** min(item['attempts'] - 1, 6), 300)

    def due(self, key, states, connected_only=False):
        items = [(identity, item) for identity, item in self.ctx.state.get(key, {}).items()
                 if item['state'] in states and item.get('nextAttemptAt', 0) <= self.now()
                 and (not connected_only or self.connected_delivery(item))]
        return sorted(items, key=lambda pair: (pair[1].get('nextAttemptAt', 0), pair[1].get('at', 0), pair[0]))[:2]

    async def flush_inbound(self, identity, item):
        outgoing = self.outgoing_reply(item['payload'])
        if not outgoing or outgoing['state'] != 'acknowledged':
            # A phone reply can arrive while its saved platform receipt is not yet
            # acknowledged. Never turn that temporary ordering into a stale refusal.
            self.defer(item)
            self.save_inbound(identity, item)
            return
        try:
            receipt = await self.transport.post('respond', item['payload'])
            if (not isinstance(receipt, dict) or not isinstance(receipt.get('replyQitemId'), str)
                    or not receipt['replyQitemId'] or type(receipt.get('complete')) is not bool):
                raise ValueError('Native response receipt is malformed.')
            item['state'] = 'recorded'
            item['receipt'] = receipt
            if receipt['complete']:
                outgoing['resolvedAt'] = self.now()
                self.save_outbound(self.outbound_key(outgoing), outgoing)
            self.save_inbound(identity, item)
        except asyncio.CancelledError:
            raise
        except RelayFailure as error:
            item['error'] = str(error)
            if error.status in (400, 401, 403, 404, 409):
                item['state'] = 'refused'
            else:
                self.defer(item)
            self.save_inbound(identity, item)
        except Exception as error:
            # Invalid JSON/200 shape and postcommit storage failure are uncertainty,
            # never proof of refusal or a reason to alter the original response tuple.
            item['state'] = 'pending'
            item.pop('receipt', None)
            item['error'] = type(error).__name__
            self.defer(item)
            self.save_inbound(identity, item)

    def prune_terminal(self, now=None):
        # Retain all undelivered questions and pending replies. Native outbox/queue
        # audit remains authoritative after the plugin's aged terminal cache is removed.
        cutoff = (self.now() if now is None else now) - 30 * 86400
        incoming = self.ctx.state.get('inbound', {})
        pending = {v['payload']['qitemId'] for v in incoming.values() if v['state'] == 'pending'}
        outgoing = self.ctx.state.get('outbound', {})
        outgoing = {k: v for k, v in outgoing.items()
                    if not (not v.get('ack') and v.get('resolvedAt', float('inf')) < cutoff
                            and v['state'] == 'acknowledged' and v['delivery']['qitemId'] not in pending)}
        incoming = {k: v for k, v in incoming.items()
                    if not (v['state'] in ('recorded', 'refused') and v['at'] < cutoff)}
        self.ctx.state.set('outbound', outgoing)
        self.ctx.state.set('inbound', incoming)

    async def poll_once(self):
        async with self.poll_lock:
            self.prune_terminal()
            # Ack before answer. Both retry lanes are bounded and rotate by due time;
            # old offline jobs cannot consume an unbounded gateway turn.
            for identity, item in self.due('outbound', ('sending', 'platform-sent', 'not-attempted-pending')):
                await self.flush_platform_receipt(item)
            for identity, item in self.due('inbound', ('pending',)):
                await self.flush_inbound(identity, item)
            notices = [(identity, item) for identity, item in self.ctx.state.get('inbound', {}).items()
                       if item.get('noticeAttemptedState') != item['state']]
            for identity, item in sorted(notices, key=lambda pair: (pair[1]['at'], pair[0]))[:2]:
                self.schedule_notice(identity)
            for identity, item in self.due('outbound', ('queued',), connected_only=True):
                await self.send_delivery(item)
            if self.delivery_mode == 'webhook-bridge':
                await self.poll_bridge()
                return
            if self.delivery_mode == 'webhook':
                return  # Durable admission replaces pull, but all retry lanes remain active.
            if not any(a and a.is_connected for a in [self.adapter, *self.adapters.values()]):
                return
            selected = self.hosts[self.host_cursor:self.host_cursor + 1]
            self.host_cursor = (self.host_cursor + 1) % max(1, len(self.hosts))
            for host in selected:
                try:
                    batch = await self.transport.post('pull', {'hostId': host, 'connectorId': self.connector, 'profile': self.profile, 'limit': 1})
                except (OSError, TimeoutError, RelayFailure):
                    continue
                for delivery in batch['deliveries']:
                    previous = self.ctx.state.get('outbound', {}).get(delivery['deliveryId'])
                    if previous and previous['state'] not in ('not-attempted',):
                        continue
                    item = {'delivery': delivery, 'hostId': host, 'state': 'queued', 'at': self.now()}
                    self.save_outbound(delivery['deliveryId'], item)
                    await self.send_delivery(item)

    async def poll_bridge(self):
        # One host/channel pair per tick; every configured pair gets bounded service.
        pairs = [(host, channel) for host in self.hosts for channel in self.bridge.channels]
        host, channel = pairs[self.bridge_cursor % len(pairs)]
        self.bridge_cursor = (self.bridge_cursor + 1) % len(pairs)
        try:
            batch = await self.transport.post('webhook-pull', {'hostId': host, 'connectorId': self.connector,
                                             'profile': self.profile, 'channel': channel, 'limit': 2})
            events = batch['events'][:2]
        except asyncio.CancelledError:
            raise
        except Exception:
            return
        for event in events:
            try:
                await self.bridge.post(event)
                delivery = event['delivery']
                await self.transport.post('webhook-accepted', {
                    'hostId': delivery['originHostId'], 'connectorId': self.connector,
                    'profile': self.profile, 'channel': delivery.get('channel', 'whatsapp'),
                    'chatId': delivery['chatId'], 'senderId': delivery['senderId'],
                    'deliveryId': event['deliveryId'], 'attemptId': delivery['attemptId'],
                    'fingerprint': event['fingerprint']})
            except asyncio.CancelledError:
                raise
            except Exception:
                # Origin retains the immutable event; the local receiver deduplicates
                # any lost local HTTP/native acceptance acknowledgment on the next poll.
                continue

    def connected_delivery(self, item):
        adapter = self.platform_adapter(item['delivery'].get('channel', 'whatsapp'))
        return adapter is not None and adapter.is_connected

    async def send_delivery(self, item):
        delivery = item['delivery']
        channel = delivery.get('channel', 'whatsapp')
        adapter = self.platform_adapter(channel)
        if not adapter or not adapter.is_connected:
            return
        try:
            result = await self.transport.post('validate', {**delivery, 'hostId': item['hostId']})
            if not isinstance(result, dict) or type(result.get('current')) is not bool:
                raise ValueError('Native validation is malformed.')
        except asyncio.CancelledError:
            raise
        except RelayFailure as error:
            item['error'] = str(error)
            if error.status == 409:
                item['state'] = 'suppressed'  # Native attempt, revision or binding is no longer valid.
            else:
                self.defer(item)
            self.save_outbound(self.outbound_key(item), item)
            return
        except Exception:
            self.defer(item)
            self.save_outbound(self.outbound_key(item), item)
            return  # Origin unavailable: retain the exact immutable attempt.
        if not result['current']:
            item['state'] = 'suppressed'
            self.save_outbound(self.outbound_key(item), item)
            return
        if not adapter.is_connected:
            return
        item['state'] = 'sending'
        self.save_outbound(self.outbound_key(item), item)  # Persist BEFORE any platform I/O.
        try:
            result = await adapter.send(delivery['chatId'], render_question(delivery))
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.flush_platform_receipt(item)
            return
        # Only the verified WhatsApp bridge's exact refusal proves no platform I/O.
        if channel == 'whatsapp' and not result.success and getattr(result, 'error', None) == 'Not connected':
            item['state'] = 'queued' if item.get('ack') else 'not-attempted-pending'
            self.defer(item)
            self.save_outbound(self.outbound_key(item), item)
            if not item.get('ack'):
                await self.flush_platform_receipt(item)
            return
        ids = list(getattr(result, 'continuation_message_ids', ()) or ())
        if getattr(result, 'message_id', None):
            ids.append(result.message_id)
        if result.success and ids:
            item['messageIds'] = ids
            item['state'] = 'platform-sent'
            self.save_outbound(self.outbound_key(item), item)
        await self.flush_platform_receipt(item)

    def known_question(self, quoted_id, channel='whatsapp', binding=None):
        matches = [item for item in self.ctx.state.get('outbound', {}).values()
                   if quoted_id and quoted_id in item.get('messageIds', [])
                   and item['delivery'].get('channel', 'whatsapp') == channel]
        if binding is not None:
            matches = [item for item in matches if all(_binding_matches(item['delivery'], binding, channel).values())]
        return matches[0] if len(matches) == 1 else None

    def quoted_id(self, event):
        quoted = getattr(event, 'reply_to_message_id', None)
        if quoted:
            return quoted
        # SDK71 omits the structured id for native stanza-only WhatsApp quotes.
        raw = getattr(event, 'raw_message', None)
        if (getattr(getattr(event.source, 'platform', None), 'value', None) == 'whatsapp'
                and isinstance(raw, dict) and isinstance(raw.get('quotedMessageId'), str)
                and raw.get('messageId') == event.message_id
                and raw.get('chatId') == event.source.chat_id
                and raw.get('senderId') == event.source.user_id):
            return raw['quotedMessageId'] or None
        return None

    def command_text(self, event):
        text = str(getattr(event, 'text', '')).strip()
        raw = getattr(event, 'raw_message', None)
        metadata = getattr(event, 'metadata', None)
        # SDK71 prefixes owner messages; strip only its identity-matched native marker.
        if (getattr(getattr(event.source, 'platform', None), 'value', None) == 'whatsapp'
                and isinstance(metadata, dict) and metadata.get('whatsapp_from_owner') is True
                and isinstance(raw, dict) and raw.get('fromOwner') is True
                and raw.get('messageId') == event.message_id
                and raw.get('chatId') == event.source.chat_id
                and raw.get('senderId') == event.source.user_id
                and text.startswith('[owner reply] ')):
            return text[len('[owner reply] '):].strip()
        return text

    async def handle_event(self, event, **kwargs):
        try:
            return await self._handle_event(event, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception:
            source = getattr(event, 'source', None)
            platform = getattr(getattr(source, 'platform', None), 'value', None)
            command = self.command_text(event).split(maxsplit=1)
            if (platform in ('whatsapp', 'slack', 'teams') and self.quoted_id(event)
                    and command and command[0] in ('/choose', '/answer', '/ask')):
                return {'action': 'skip', 'reason': 'OpenRig storage or transport failed; no success is claimed. The original reply remains retained if it was persisted.'}
            return None

    async def _handle_event(self, event, **kwargs):
        source = event.source
        channel = getattr(getattr(source, 'platform', None), 'value', None)
        if channel not in ('whatsapp', 'slack', 'teams'):
            return None
        quoted_id = self.quoted_id(event)
        profile = getattr(source, 'profile', None) or self.profile
        binding = (profile, source.chat_id, source.user_id)
        item = self.known_question(quoted_id, channel, binding)
        if not item:
            candidate = self.known_question(quoted_id, channel)
            if candidate:
                matches = _binding_matches(candidate['delivery'], binding, channel)
                diagnostic = ', '.join(f'{key}={value}' for key, value in matches.items())
                return {'action': 'skip', 'reason': 'OpenRig question belongs to a different bound sender/profile/chat. ' + diagnostic}
            if any(quoted_id and quoted_id in value.get('messageIds', [])
                   and value['delivery'].get('channel', 'whatsapp') == channel
                   for value in self.ctx.state.get('outbound', {}).values()):
                return {'action': 'skip', 'reason': 'OpenRig quoted question is ambiguous; no answer was queued.'}
            return None
        d = item['delivery']
        text = self.command_text(event)
        base = {'hostId': item['hostId'], 'deliveryId': d['deliveryId'], 'connectorId': d['connectorId'],
                'profile': d['profile'], 'chatId': d['chatId'], 'senderId': d['senderId'],
                'replyToMessageId': quoted_id}
        if channel != 'whatsapp':
            base['channel'] = channel
        command = text.split(maxsplit=1)[0] if text else ''
        if command not in ('/choose', '/answer', '/ask'):
            try:
                context = await self.transport.post('context', base)
                label = 'Current' if context.get('current') else 'Changed or closed'
            except (OSError, TimeoutError, RelayFailure):
                context = d
                label = 'Historical snapshot; native OpenRig is unavailable'
            return {'action': 'rewrite', 'text': event.text.strip() + '\n\n[Read-only OpenRig context: ' + label + '. '
                    'No answer is authorized or recorded by this conversation. Use an explicit quoted /choose, /answer, or /ask command to submit.]\n' + json.dumps(context, ensure_ascii=False)}
        try:
            argument = text[len(command):].strip()
            if command == '/choose':
                numbers = [int(part) for part in argument.split()]
                if len(numbers) == 1 and len(d['questions']) == 1:
                    numbers = [1, numbers[0]]
                if len(numbers) != 2 or any(n < 1 for n in numbers):
                    raise ValueError('Choose a question and option number.')
                question = d['questions'][numbers[0] - 1]
                option = question['options'][numbers[1] - 1]
                response = {'kind': 'choice', 'questionId': question['id'], 'optionId': option['id']}
            else:
                if not argument:
                    raise ValueError('An explicit answer or clarification is required.')
                response = {'kind': 'custom' if command == '/answer' else 'clarification', 'text': argument}
            if not event.message_id:
                raise ValueError('Native inbound message identity is required.')
        except (ValueError, IndexError):
            return {'action': 'skip', 'reason': 'Malformed OpenRig reply; no answer was queued.'}
        payload = {**base, 'messageId': event.message_id, 'qitemId': d['qitemId'],
                   'requestRevision': d['requestRevision'], 'response': response}
        identity = hashlib.sha256(json.dumps([channel, d['profile'], d['chatId'], d['senderId'], event.message_id], separators=(',', ':')).encode()).hexdigest()
        async with self.inbound_lock:
            existing = self.ctx.state.get('inbound', {}).get(identity)
            if existing:
                return {'action': 'skip', 'reason': 'This native message already has a durable OpenRig reply record.'}
            record = {'payload': payload, 'state': 'pending', 'at': self.now()}
            if (profile, source.chat_id, source.user_id) != (d['profile'], d['chatId'], d['senderId']):
                record['observedSource'] = {'profile': profile, 'chatId': source.chat_id, 'senderId': source.user_id}
            try:
                self.save_inbound(identity, record)  # Durable BEFORE the native HTTP call.
            except Exception:
                self.schedule_notice(identity, failure=(source.chat_id, event.message_id, channel))
                return {'action': 'skip', 'reason': 'Durable storage refused the reply; it was not queued.'}
        self.schedule_notice(identity)
        return {'action': 'skip', 'reason': 'OpenRig reply ' + record['state'] + '; original request identity preserved.'}


def register(ctx):
    plugin = RelayPlugin(ctx)
    ctx.register_platform_handler('whatsapp', plugin.attach_whatsapp)
    ctx.register_platform_handler('webhook', plugin.attach_webhook)
    for channel in ('slack', 'teams'):
        ctx.register_platform_handler(channel, lambda native, adapter, channel=channel: plugin.attach_channel(channel, native, adapter))
    ctx.register_hook('openrig-human-relay', plugin.handle_webhook)
    ctx.register_hook('pre_gateway_dispatch', plugin.handle_event)
