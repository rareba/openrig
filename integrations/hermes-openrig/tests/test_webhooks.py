"""Durable HTTP admission is separate from native platform submission."""
import copy
import hashlib
import json
import types
import unittest
from test_plugin import Context, Transport, Adapter, DELIVERY, event, module


def envelope(delivery=None):
 d=copy.deepcopy(delivery or DELIVERY)
 return {'schema':'openrig.human-relay.v1','type':'question.created','deliveryId':d['deliveryId'],
         'fingerprint':hashlib.sha256(json.dumps(d,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),'delivery':d}

class WebhookTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.ctx=Context();original=self.ctx.get_config
  self.ctx.get_config=lambda k,d=None: 'webhook' if k=='delivery_mode' else original(k,d)
  self.transport=Transport();self.clock=10000
  self.plugin=module.RelayPlugin(self.ctx,self.transport,now=lambda:self.clock)
  self.adapter=Adapter();self.plugin.adapter=self.adapter
 async def admit(self,payload=None,**kwargs):
  return await self.plugin.handle_webhook(route_name='openrig-human-relay',profile='default',payload=payload or envelope(),delivery_id=kwargs.get('delivery_id',DELIVERY['deliveryId']))
 async def test_durable_accept_duplicate_lost_ack_and_restart_send_once(self):
  result=await self.admit();self.assertEqual(result,{'status':200,'body':{'schema':'openrig.human-relay.v1','status':'accepted','deliveryId':DELIVERY['deliveryId'],'fingerprint':envelope()['fingerprint']}})
  self.assertEqual(self.adapter.sent,[])
  restarted=module.RelayPlugin(self.ctx,self.transport,now=lambda:self.clock);restarted.adapter=self.adapter
  self.assertEqual(await restarted.handle_webhook(route_name='openrig-human-relay',profile='default',payload=envelope(),delivery_id=DELIVERY['deliveryId']),result)
  await restarted.poll_once();await restarted.poll_once()
  self.assertEqual(len(self.adapter.sent),1);self.assertFalse(any(op=='pull' for op,p in self.transport.calls))
  self.assertEqual(await self.admit(),result)
 async def test_id_conflict_and_fingerprint_tampering(self):
  await self.admit();changed=copy.deepcopy(DELIVERY);changed['body']='changed'
  self.assertEqual((await self.admit(envelope(changed)))['status'],409)
  p=envelope();p['delivery']['body']='tampered';self.assertEqual((await self.admit(p))['status'],400)
 async def test_origin_scoped_identity(self):
  await self.admit();d=copy.deepcopy(DELIVERY);d['originHostId']='other-origin'
  self.assertEqual((await self.admit(envelope(d)))['status'],200)
  self.assertEqual(len(self.ctx.state.get('outbound')),2)
 async def test_route_profile_header_and_schema_are_pinned(self):
  for changes in ({'route_name':'elsewhere'},{'profile':'other'},{'delivery_id':'other'}):
   args=dict(route_name='openrig-human-relay',profile='default',payload=envelope(),delivery_id=DELIVERY['deliveryId']);args.update(changes)
   self.assertIn((await self.plugin.handle_webhook(**args))['status'],(400,403))
  for changes in ({'schema':'elsewhere'},{'type':'answer.created'},{'endpoint':'http://untrusted'}):
   p=envelope();p.update(changes);self.assertEqual((await self.admit(p))['status'],400)
  self.assertEqual(self.ctx.state.get('outbound',{}),{})
 async def test_storage_failure_cannot_ack_or_send(self):
  def refused(*args):raise OSError('full')
  self.ctx.state.set=refused
  self.assertEqual((await self.admit())['status'],503);self.assertEqual(self.adapter.sent,[])
 async def test_offline_acceptance_validates_current_only_at_drain(self):
  self.adapter.is_connected=False;await self.admit();await self.plugin.poll_once();self.assertEqual(self.transport.calls,[])
  self.adapter.is_connected=True;await self.plugin.poll_once()
  self.assertEqual(self.transport.calls[0][0],'validate');self.assertEqual(self.transport.calls[0][1]['hostId'],'origin');self.assertEqual(len(self.adapter.sent),1)
 async def test_origin_unavailable_holds_then_changed_suppresses(self):
  await self.admit();self.transport.online=False;await self.plugin.poll_once();self.assertEqual(self.adapter.sent,[])
  self.clock+=61
  async def stale(op,p):return {'current':False,'reason':'stale_human_request'}
  self.transport.post=stale;await self.plugin.poll_once();self.assertEqual(self.adapter.sent,[])
  self.assertEqual(next(iter(self.ctx.state.get('outbound').values()))['state'],'suppressed')
 async def test_sending_after_crash_is_indeterminate_and_never_reposts(self):
  await self.admit();saved=self.ctx.state.get('outbound');next(iter(saved.values()))['state']='sending';self.ctx.state.set('outbound',saved)
  await self.plugin.poll_once();self.assertEqual(self.adapter.sent,[])
  self.assertEqual(next(iter(self.ctx.state.get('outbound').values()))['state'],'indeterminate')
 async def test_slack_and_teams_native_quotes_use_channel_adapter_and_identity(self):
  for channel in ('slack','teams'):
   d=copy.deepcopy(DELIVERY);d['channel']=channel;d['deliveryId']+='-'+channel
   adapter=Adapter();self.plugin.attach_channel(channel,None,adapter);self.plugin.stop()
   await self.admit(envelope(d),delivery_id=d['deliveryId']);await self.plugin.poll_once()
   self.assertEqual(len(adapter.sent),1)
   e=event();e.source.platform.value=channel;e.message_id='same-native-id'
   result=await self.plugin.handle_event(e);self.assertEqual(result['action'],'skip')
   record=list(self.ctx.state.get('inbound').values())[-1]
   self.assertEqual(record['payload']['channel'],channel);self.assertEqual(record['payload']['replyToMessageId'],'platform-1')
  self.assertEqual(len(self.ctx.state.get('inbound')),2)
 async def test_other_channel_same_quote_cannot_authorize_question(self):
  await self.admit();await self.plugin.poll_once();e=event();e.source.platform.value='slack'
  self.assertIsNone(await self.plugin.handle_event(e));self.assertEqual(self.ctx.state.get('inbound',{}),{})
 async def test_native_binding_conflict_suppresses_without_platform_send(self):
  await self.admit()
  async def refused(op,p):raise module.RelayFailure(409,'relay_binding_changed')
  self.transport.post=refused;await self.plugin.poll_once()
  self.assertEqual(self.adapter.sent,[])
  self.assertEqual(next(iter(self.ctx.state.get('outbound').values()))['state'],'suppressed')
 async def test_sending_claim_storage_failure_prevents_platform_io(self):
  await self.admit();original=self.ctx.state.set
  def refuse(key,value):
   if key=='outbound' and any(v['state']=='sending' for v in value.values()):raise OSError('disk full')
   return original(key,value)
  self.ctx.state.set=refuse
  with self.assertRaises(OSError):await self.plugin.poll_once()
  self.assertEqual(self.adapter.sent,[])
 async def test_concurrent_callbacks_share_one_durable_entry(self):
  import asyncio
  results=await asyncio.gather(*(self.admit() for _ in range(8)))
  self.assertTrue(all(r==results[0] for r in results));self.assertEqual(len(self.ctx.state.get('outbound')),1)
 async def test_native_profile_default_and_webhook_offline_recovery(self):
  await self.admit();original=self.adapter.send
  async def offline(*args,**kwargs):return types.SimpleNamespace(success=False,error='Not connected')
  self.adapter.send=offline;await self.plugin.poll_once()
  self.assertEqual(next(iter(self.ctx.state.get('outbound').values()))['state'],'queued')
  self.assertFalse(any(op=='receipt' for op,p in self.transport.calls))
  self.adapter.send=original;self.clock+=61;await self.plugin.poll_once()
  self.assertEqual(len(self.adapter.sent),1)
 async def test_slack_reply_and_notices_stay_on_native_channel(self):
  import asyncio
  d=copy.deepcopy(DELIVERY);d['channel']='slack'
  slack=Adapter();self.plugin.adapters['slack']=slack
  await self.admit(envelope(d));await self.plugin.poll_once()
  e=event('/answer Only synthetic data');e.source.platform.value='slack'
  await self.plugin.handle_event(e);await asyncio.sleep(0)
  self.assertEqual(self.adapter.sent,[]);self.assertIn('queued',slack.sent[-1][1])
  await self.plugin.poll_once();await asyncio.sleep(0)
  self.assertIn('recorded your answer',slack.sent[-1][1]);self.assertEqual(self.adapter.sent,[])
  payload=next(p for op,p in self.transport.calls if op=='respond')
  self.assertEqual(payload['channel'],'slack');self.assertEqual(payload['senderId'],'person')
 async def test_channel_text_actor_and_whatsapp_raw_fallback_are_not_trusted_on_slack(self):
  d=copy.deepcopy(DELIVERY);d['channel']='slack';self.plugin.adapters['slack']=Adapter()
  await self.admit(envelope(d));await self.plugin.poll_once()
  e=event('/answer senderId=person',sender='other');e.source.platform.value='slack'
  self.assertEqual((await self.plugin.handle_event(e))['action'],'skip');self.assertEqual(self.ctx.state.get('inbound',{}),{})
  e=event(quote=None);e.source.platform.value='slack';e.raw_message={'quotedMessageId':'platform-1','messageId':e.message_id,'chatId':'chat','senderId':'person'}
  self.assertIsNone(await self.plugin.handle_event(e))
 async def test_unicode_canonical_delivery_and_malformed_current_hold(self):
  d=copy.deepcopy(DELIVERY);d['summary']='Question café 船'
  self.assertEqual((await self.admit(envelope(d)))['status'],200)
  async def malformed(op,p):return {'current':'true'}
  self.transport.post=malformed;await self.plugin.poll_once()
  self.assertEqual(self.adapter.sent,[]);self.assertEqual(next(iter(self.ctx.state.get('outbound').values()))['state'],'queued')
 async def test_offline_channel_backlog_does_not_starve_connected_provider(self):
  self.adapter.is_connected=False
  for n in range(3):
   d=copy.deepcopy(DELIVERY);d['deliveryId']+=str(n);await self.admit(envelope(d),delivery_id=d['deliveryId'])
  d=copy.deepcopy(DELIVERY);d['channel']='slack';d['deliveryId']+='slack'
  slack=Adapter();self.plugin.adapters['slack']=slack;await self.admit(envelope(d),delivery_id=d['deliveryId'])
  saved=self.ctx.state.get('outbound')
  for item in saved.values():item['nextAttemptAt']=1 if item['delivery'].get('channel')=='slack' else 0
  self.ctx.state.set('outbound',saved)
  await self.plugin.poll_once();self.assertEqual(len(slack.sent),1);self.assertEqual(self.adapter.sent,[])
 async def test_same_platform_receipt_in_other_chat_matches_full_native_binding(self):
  for channel in ('slack','teams'):
   for chat in ('chat','different-chat'):
    d=copy.deepcopy(DELIVERY);d['channel']=channel;d['chatId']=chat;d['deliveryId']+='-'+channel+'-'+chat
    self.plugin.adapters[channel]=Adapter()
    await self.admit(envelope(d),delivery_id=d['deliveryId']);await self.plugin.poll_once()
   e=event('/answer Bound native answer');e.source.platform.value=channel;e.source.chat_id='different-chat';e.message_id='reply-'+channel
   result=await self.plugin.handle_event(e);self.assertEqual(result['action'],'skip')
   records=list(self.ctx.state.get('inbound',{}).values())
   self.assertTrue(any(r['payload']['channel']==channel and r['payload']['deliveryId'].endswith('-different-chat') for r in records))
 async def test_unstructured_native_request_preserves_quoted_custom_answer(self):
  d=copy.deepcopy(DELIVERY);d['questions']=[]
  self.assertEqual((await self.admit(envelope(d)))['status'],200)
  await self.plugin.poll_once()
  self.assertEqual((await self.plugin.handle_event(event('/answer Explicit unstructured answer')))['action'],'skip')
  await self.plugin.poll_once()
  record=next(iter(self.ctx.state.get('inbound').values()))
  self.assertEqual(record['state'],'recorded')
  self.assertEqual(record['payload']['response'],{'kind':'custom','text':'Explicit unstructured answer'})
 async def test_registers_exact_async_hook_and_all_supported_platforms(self):
  module.register(self.ctx)
  self.assertIn('openrig-human-relay',self.ctx.hooks)
  self.assertTrue({'whatsapp','slack','teams','webhook'}<=self.ctx.platforms.keys())
