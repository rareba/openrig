import asyncio
import copy
import importlib.util
import pathlib
import types
import unittest

spec=importlib.util.spec_from_file_location('openrig_plugin',pathlib.Path(__file__).parents[1]/'__init__.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

DELIVERY={'deliveryId':'human-relay-test','attemptId':'attempt-1','originHostId':'origin','qitemId':'question-1','requestRevision':'original-revision','connectorId':'hermes','humanSession':'human@kernel','profile':'default','chatId':'chat','senderId':'person','summary':'A synthetic question','body':'Synthetic context only','evidenceRef':'/proof','questions':[{'id':'sample','question':'Which sample?','options':[{'id':'blue','label':'Blue','recommended':True,'consequences':'Test data only.'},{'id':'green','label':'Green'}]}]}
class State:
 def __init__(self):self.data={}
 def get(self,key,default=None):return copy.deepcopy(self.data.get(key,default))
 def set(self,key,value):self.data[key]=copy.deepcopy(value)
class Context:
 profile_name='default'
 def __init__(self):self.state=State();self.hooks={};self.platforms={};self.unload=[]
 def get_config(self,key,default=None):return {'connector_id':'hermes','origin_hosts':['local'],'openrig_url':'http://127.0.0.1:17433'}.get(key,default)
 def on_unload(self,callback):self.unload.append(callback)
 def spawn_task(self,coro,*,name=None):return asyncio.create_task(coro,name=name)
 def register_hook(self,name,callback):self.hooks[name]=callback
 def register_platform_handler(self,name,callback):self.platforms[name]=callback
class Transport:
 def __init__(self):self.calls=[];self.online=True;self.claimed=False;self.lost_response=False
 async def post(self,operation,payload):
  self.calls.append((operation,copy.deepcopy(payload)))
  if not self.online:raise OSError('offline')
  if operation=='pull':
   if self.claimed:return {'deliveries':[]}
   self.claimed=True;return {'deliveries':[copy.deepcopy(DELIVERY)]}
  if operation=='respond' and self.lost_response:self.lost_response=False;raise OSError('HTTP acknowledgment lost after native commit')
  if operation=='context':return {'current':True,'body':'Current native context','questions':DELIVERY['questions']}
  return {'state':'delivered' if payload.get('outcome')=='platform-sent' else ('pending' if payload.get('outcome')=='not-attempted' else 'indeterminate')} if operation=='receipt' else {'replyQitemId':'native-reply','complete':payload.get('response',{}).get('kind')!='clarification'}
class Adapter:
 is_connected=True
 def __init__(self):self.sent=[];self.ambiguous=False
 async def send(self,chat,content,**kwargs):
  self.sent.append((chat,content))
  if self.ambiguous:raise TimeoutError('unknown platform outcome')
  return types.SimpleNamespace(success=True,message_id='platform-1',continuation_message_ids=())
def event(text='/choose 1',sender='person',quote='platform-1'):
 return types.SimpleNamespace(text=text,message_id='inbound-1',reply_to_message_id=quote,source=types.SimpleNamespace(platform=types.SimpleNamespace(value='whatsapp'),profile='default',chat_id='chat',user_id=sender))
class PluginTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.ctx=Context();self.transport=Transport();self.clock=10000;self.now=lambda:self.clock;self.plugin=module.RelayPlugin(self.ctx,self.transport,now=self.now);self.adapter=Adapter();self.plugin.adapter=self.adapter
 async def test_disconnected_adapter_never_claims_and_recovers_when_online(self):
  self.adapter.is_connected=False;await self.plugin.poll_once();self.assertEqual(self.transport.calls,[])
  self.adapter.is_connected=True;await self.plugin.poll_once();self.assertEqual(len(self.adapter.sent),1);self.assertIn('Test data only.',self.adapter.sent[0][1]);self.assertIn('/answer',self.adapter.sent[0][1])
 async def test_proven_pre_send_bridge_offline_remains_retryable(self):
  async def offline(chat,content,**kwargs):return types.SimpleNamespace(success=False,error='Not connected',message_id=None,continuation_message_ids=())
  self.adapter.send=offline;await self.plugin.poll_once()
  self.assertTrue(any(op=='receipt' and p['outcome']=='not-attempted' for op,p in self.transport.calls))
  self.assertFalse(any(op=='receipt' and p['outcome']=='unknown' for op,p in self.transport.calls))
 async def test_unknown_platform_outcome_is_held_without_resend_after_restart(self):
  self.adapter.ambiguous=True;await self.plugin.poll_once();restarted=module.RelayPlugin(self.ctx,self.transport,now=self.now);restarted.attach_whatsapp(None,self.adapter);await restarted.poll_once();self.assertEqual(len(self.adapter.sent),1)
  self.assertTrue(any(op=='receipt' and p['outcome']=='unknown' for op,p in self.transport.calls))
 async def test_saved_platform_receipt_retries_ack_only(self):
  original=self.transport.post
  async def post(op,p):
   if op=='receipt' and self.transport.online:self.transport.online=False;raise OSError('ack lost')
   return await original(op,p)
  self.plugin.transport.post=post;await self.plugin.poll_once();self.plugin.transport.post=original;self.transport.online=True
  restarted=module.RelayPlugin(self.ctx,self.transport,now=self.now);restarted.attach_whatsapp(None,self.adapter);await restarted.poll_once();self.assertEqual(len(self.adapter.sent),1)
 async def test_inbound_is_durable_before_http_and_retries_same_native_tuple(self):
  await self.plugin.poll_once();self.transport.lost_response=True
  result=await self.plugin.handle_event(event());self.assertEqual(result['action'],'skip');await self.plugin.poll_once()
  first=[p for op,p in self.transport.calls if op=='respond'][-1];self.assertEqual(first['response'],{'kind':'choice','questionId':'sample','optionId':'blue'});self.assertEqual(first['requestRevision'],'original-revision')
  restarted=module.RelayPlugin(self.ctx,self.transport,now=self.now);restarted.attach_whatsapp(None,self.adapter);self.clock+=61;await restarted.poll_once()
  replies=[p for op,p in self.transport.calls if op=='respond'];self.assertEqual(replies[-1],first)
  await restarted.handle_event(event());self.assertEqual(len([p for op,p in self.transport.calls if op=='respond']),2)
 async def test_clarification_and_custom_remain_explicit_and_wrong_sender_never_reaches_model(self):
  await self.plugin.poll_once();result=await self.plugin.handle_event(event('/answer No release authority',sender='other'));self.assertEqual(result['action'],'skip');self.assertFalse(any(op=='respond' for op,p in self.transport.calls))
  e=event('/ask Which sample?');await self.plugin.handle_event(e);await self.plugin.poll_once();self.assertEqual([p for op,p in self.transport.calls if op=='respond'][-1]['response'],{'kind':'clarification','text':'Which sample?'})
  e=event('/answer Only synthetic data');e.message_id='inbound-2';await self.plugin.handle_event(e);await self.plugin.poll_once();self.assertEqual([p for op,p in self.transport.calls if op=='respond'][-1]['response'],{'kind':'custom','text':'Only synthetic data'})
 async def test_conversation_uses_readonly_current_context_without_submitting_an_answer(self):
  await self.plugin.poll_once();result=await self.plugin.handle_event(event('What are the alternatives?'));self.assertEqual(result['action'],'rewrite');self.assertIn('Current native context',result['text']);self.assertFalse(any(op=='respond' for op,p in self.transport.calls))
  self.assertIsNone(await self.plugin.handle_event(event('Ordinary chat',quote=None)))
 async def test_native_stanza_only_quote_remains_linked_and_unquoted_input_is_not_inferred(self):
  await self.plugin.poll_once()
  e=event(quote=None)
  self.assertIsNone(await self.plugin.handle_event(e));self.assertEqual(self.ctx.state.get('inbound',{}),{})
  # SDK71 drops the structured reply id when quoted text is absent, but retains
  # the native bridge stanza id and its event identity in raw_message.
  e.raw_message={'hasQuotedMessage':False,'quotedMessageId':'platform-1','messageId':e.message_id,'chatId':'chat','senderId':'person'}
  result=await self.plugin.handle_event(e);self.assertIsNotNone(result);self.assertEqual(result['action'],'skip')
  saved=list(self.ctx.state.get('inbound').values())[0]['payload']
  self.assertEqual(saved['replyToMessageId'],'platform-1');self.assertEqual(saved['qitemId'],'question-1')
  self.assertEqual(saved['response'],{'kind':'choice','questionId':'sample','optionId':'blue'})
 async def test_raw_quote_requires_matching_native_message_chat_and_sender(self):
  await self.plugin.poll_once()
  for field in ('messageId','chatId','senderId'):
   e=event(quote=None);e.raw_message={'quotedMessageId':'platform-1','messageId':e.message_id,'chatId':'chat','senderId':'person',field:'different'}
   self.assertIsNone(await self.plugin.handle_event(e))
  self.assertEqual(self.ctx.state.get('inbound',{}),{})
 async def test_terminal_stale_native_response_is_retained_and_never_retargeted(self):
  await self.plugin.poll_once();original=self.transport.post
  async def refused(operation,payload):
   if operation=='respond':raise module.RelayFailure(409,'stale_human_request')
   return await original(operation,payload)
  self.transport.post=refused;await self.plugin.handle_event(event('/answer Old answer'));await self.plugin.poll_once();saved=list(self.ctx.state.get('inbound').values())[0]
  self.assertEqual(saved['state'],'refused');self.assertEqual(saved['payload']['requestRevision'],'original-revision');self.assertEqual(saved['error'],'stale_human_request')
 async def test_retention_prunes_only_aged_confirmed_terminal_state(self):
  self.ctx.state.set('outbound',{'resolved':{'state':'acknowledged','resolvedAt':1,'delivery':DELIVERY},'pending':{'state':'sending','at':1,'delivery':DELIVERY}})
  self.ctx.state.set('inbound',{'done':{'state':'recorded','at':1,'payload':{'qitemId':'question-1'}},'pending':{'state':'pending','at':1,'payload':{'qitemId':'question-2'}}})
  self.plugin.prune_terminal(now=4000000)
  self.assertIn('pending',self.ctx.state.get('outbound'));self.assertIn('pending',self.ctx.state.get('inbound'))
  self.assertNotIn('resolved',self.ctx.state.get('outbound'));self.assertNotIn('done',self.ctx.state.get('inbound'))
 async def test_reply_waits_for_saved_platform_receipt_ack_before_native_submission(self):
  original=self.transport.post;acked=False;allow_ack=False;offline=False
  async def post(operation,payload):
   nonlocal acked
   if offline:raise OSError('origin offline')
   if operation=='receipt':
    if not allow_ack:raise OSError('platform receipt acknowledgment lost')
    acked=True
   if operation=='respond' and not acked:raise module.RelayFailure(409,'platform_receipt_not_acknowledged')
   return await original(operation,payload)
  self.transport.post=post;await self.plugin.poll_once();offline=True
  await self.plugin.handle_event(event('/answer Original answer'));self.assertEqual(list(self.ctx.state.get('inbound').values())[0]['state'],'pending')
  offline=False;allow_ack=True;self.clock+=61;await self.plugin.poll_once();self.assertEqual(list(self.ctx.state.get('inbound').values())[0]['state'],'recorded')
 async def test_invalid_native_receipt_cannot_mark_recorded_or_fail_command_open(self):
  await self.plugin.poll_once();original=self.transport.post
  async def invalid(operation,payload):
   if operation=='respond':return {}
   return await original(operation,payload)
  self.transport.post=invalid;result=await self.plugin.handle_event(event());self.assertEqual(result['action'],'skip');await self.plugin.poll_once()
  self.assertEqual(list(self.ctx.state.get('inbound').values())[0]['state'],'pending')
 async def test_storage_failure_during_scoped_command_never_enters_llm(self):
  await self.plugin.poll_once();original=self.ctx.state.get
  def broken(key,default=None):
   if key=='outbound':raise ValueError('corrupt state')
   return original(key,default)
  self.ctx.state.get=broken;result=await self.plugin.handle_event(event());self.assertEqual(result['action'],'skip')
 async def test_large_offline_inbound_backlog_is_bounded_and_rotates(self):
  await self.plugin.poll_once();self.transport.online=False
  for i in range(8):
   e=event('/answer Durable answer');e.message_id='inbound-'+str(i);await self.plugin.handle_event(e)
  self.transport.calls=[];self.transport.online=False;await self.plugin.poll_once()
  first=[p['messageId'] for op,p in self.transport.calls if op=='respond'];self.assertEqual(len(first),2)
  await self.plugin.poll_once();second=[p['messageId'] for op,p in self.transport.calls if op=='respond'][2:];self.assertEqual(len(second),2);self.assertFalse(set(first)&set(second))
 async def test_postcommit_state_failure_keeps_same_tuple_and_consumes_command(self):
  await self.plugin.poll_once();result=await self.plugin.handle_event(event('/answer Exact answer'));self.assertEqual(result['action'],'skip')
  original=self.ctx.state.set;failed=False
  def fail_once(key,value):
   nonlocal failed
   if key=='inbound' and not failed and any(v['state']=='recorded' for v in value.values()):
    failed=True;raise OSError('state write failed after native commit')
   return original(key,value)
  self.ctx.state.set=fail_once;await self.plugin.poll_once();saved=list(self.ctx.state.get('inbound').values())[0];self.assertEqual(saved['state'],'pending')
  self.clock+=61;await self.plugin.poll_once();self.assertEqual(list(self.ctx.state.get('inbound').values())[0]['state'],'recorded')
  attempts=[p for op,p in self.transport.calls if op=='respond'];self.assertEqual(len(attempts),2);self.assertEqual(attempts[0],attempts[1])
 async def test_definite_offline_receipt_is_durable_before_http_and_recovers_without_unknown(self):
  original_post=self.transport.post;original_send=self.adapter.send;block_receipt=True
  async def offline_send(chat,content,**kwargs):return types.SimpleNamespace(success=False,error='Not connected',message_id=None,continuation_message_ids=())
  async def post(operation,payload):
   if operation=='receipt' and payload['outcome']=='not-attempted':
    if block_receipt:raise OSError('receipt could not reach native daemon')
    self.transport.claimed=False
   return await original_post(operation,payload)
  self.transport.post=post;self.adapter.send=offline_send;await self.plugin.poll_once()
  self.assertEqual(list(self.ctx.state.get('outbound').values())[0]['state'],'not-attempted-pending')
  block_receipt=False;self.adapter.send=original_send;self.clock+=61;await self.plugin.poll_once()
  self.assertEqual(len(self.adapter.sent),1);self.assertFalse(any(op=='receipt' and p['outcome']=='unknown' for op,p in self.transport.calls))
 async def test_platform_receipt_storage_failure_stops_before_http_ack_and_never_resends(self):
  original=self.ctx.state.set
  def refuse_receipt(key,value):
   if key=='outbound' and any(v['state']=='platform-sent' for v in value.values()):raise OSError('receipt storage unavailable')
   return original(key,value)
  self.ctx.state.set=refuse_receipt
  with self.assertRaises(OSError):await self.plugin.poll_once()
  self.assertEqual(len(self.adapter.sent),1);self.assertFalse(any(op=='receipt' for op,p in self.transport.calls))
  self.ctx.state.set=original;restarted=module.RelayPlugin(self.ctx,self.transport,now=self.now);restarted.attach_whatsapp(None,self.adapter);await restarted.poll_once();self.assertEqual(len(self.adapter.sent),1)
 async def test_phone_gets_truthful_queued_then_native_receipt_confirmation(self):
  await self.plugin.poll_once();self.transport.online=False
  await self.plugin.handle_event(event('/answer Explicit answer'));await asyncio.sleep(0)
  self.assertIn('queued',self.adapter.sent[-1][1]);self.assertNotIn('recorded your answer',self.adapter.sent[-1][1])
  await self.plugin.poll_once();self.transport.online=True;self.clock+=61;await self.plugin.poll_once();await asyncio.sleep(0)
  self.assertIn('OpenRig recorded your answer',self.adapter.sent[-1][1])
 async def test_phone_sees_stale_refusal_without_a_false_success(self):
  await self.plugin.poll_once();original=self.transport.post
  async def refused(operation,payload):
   if operation=='respond':raise module.RelayFailure(409,'stale_human_request')
   return await original(operation,payload)
  self.transport.post=refused;await self.plugin.handle_event(event('/answer Old answer'));await self.plugin.poll_once();await asyncio.sleep(0)
  self.assertIn('not accept',self.adapter.sent[-1][1]);self.assertNotIn('recorded your answer',self.adapter.sent[-1][1])
 async def test_whatsapp_factory_starts_once_reconnects_and_unloads_without_api_server(self):
  module.register(self.ctx);plugin=self.ctx.hooks['pre_gateway_dispatch'].__self__;plugin.transport=self.transport
  self.assertNotIn('api_server',self.ctx.platforms)
  self.ctx.platforms['whatsapp'](None,self.adapter);first=plugin.poll_task
  replacement=Adapter();self.ctx.platforms['whatsapp'](None,replacement)
  self.assertIs(plugin.poll_task,first);self.assertIs(plugin.adapter,replacement)
  self.adapter=replacement
  await asyncio.sleep(0);self.assertEqual(len(self.adapter.sent),1)
  self.adapter.is_connected=False;await plugin.poll_once();self.assertEqual(len(self.adapter.sent),1)
  self.adapter.is_connected=True;await plugin.poll_once();self.assertEqual(len(self.adapter.sent),1)
  for callback in self.ctx.unload:callback()
  await asyncio.gather(first,return_exceptions=True);self.assertTrue(first.cancelled())
  self.assertFalse(plugin.notice_tasks)
 async def test_existing_webhook_boot_retries_saved_answer_before_whatsapp_connects(self):
  await self.plugin.poll_once();await self.plugin.handle_event(event('/answer Saved before restart'))
  module.register(self.ctx);plugin=self.ctx.hooks['pre_gateway_dispatch'].__self__;plugin.transport=self.transport
  self.transport.calls=[]
  self.ctx.platforms['webhook'](None,None);first=plugin.poll_task;await asyncio.sleep(0)
  self.assertIsNone(plugin.adapter)
  self.assertEqual(list(self.ctx.state.get('inbound').values())[0]['state'],'recorded')
  self.assertFalse(any(op=='pull' for op,p in self.transport.calls))
  self.ctx.platforms['whatsapp'](None,self.adapter);self.assertIs(plugin.poll_task,first)
  for callback in self.ctx.unload:callback()
  await asyncio.gather(first,return_exceptions=True)
 async def test_unload_cancels_inflight_notice_and_does_not_restart(self):
  module.register(self.ctx);plugin=self.ctx.hooks['pre_gateway_dispatch'].__self__;plugin.transport=self.transport
  self.ctx.platforms['whatsapp'](None,self.adapter);await asyncio.sleep(0)
  waiting=asyncio.Event()
  async def slow_send(*args,**kwargs):await waiting.wait()
  self.adapter.send=slow_send;await plugin.handle_event(event('/answer Original reply'));await asyncio.sleep(0)
  tasks=[plugin.poll_task,*plugin.notice_tasks.values()]
  for callback in self.ctx.unload:callback()
  await asyncio.gather(*tasks,return_exceptions=True)
  self.assertTrue(all(t.cancelled() for t in tasks))
  self.ctx.platforms['whatsapp'](None,self.adapter);self.assertIs(plugin.poll_task,tasks[0])
if __name__=='__main__':unittest.main()
