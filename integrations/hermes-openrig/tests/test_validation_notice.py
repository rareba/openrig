"""Invalid bound replies get guidance without recording a human answer."""
import asyncio
import copy
import unittest
from test_plugin import Adapter, Context, DELIVERY, Transport, event, module


class ValidationNoticeTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.ctx=Context();self.transport=Transport();self.plugin=module.RelayPlugin(self.ctx,self.transport);self.adapter=Adapter();self.plugin.adapter=self.adapter
  self.ctx.state.set('outbound',{'original':{'delivery':copy.deepcopy(DELIVERY),'hostId':'origin','state':'acknowledged','messageIds':['platform-1'],'at':1}})

 async def test_malformed_bound_commands_get_truthful_deduplicated_guidance(self):
  for index,text in enumerate(('/choose','/choose 99','/choose blue','/answer')):
   with self.subTest(text=text):
    e=event(text);e.message_id='invalid-'+str(index)
    result=await self.plugin.handle_event(e);await asyncio.sleep(0)
    self.assertEqual(result['action'],'skip');self.assertIn('Malformed',result['reason'])
    self.assertEqual(len(self.adapter.sent),index+1)
    notice=self.adapter.sent[-1][1];self.assertIn('not queued',notice);self.assertIn('/choose',notice);self.assertIn('/answer',notice)
    count=len(self.adapter.sent);await self.plugin.handle_event(e);await asyncio.sleep(0)
    self.assertEqual(len(self.adapter.sent),count)
  self.assertEqual(self.ctx.state.get('inbound',{}),{});self.assertEqual(self.transport.calls,[])

 async def test_unstructured_question_guidance_uses_written_answer(self):
  items=self.ctx.state.get('outbound');items['original']['delivery']['questions']=[];self.ctx.state.set('outbound',items)
  result=await self.plugin.handle_event(event('/choose 1'));await asyncio.sleep(0)
  self.assertIn('question_ref=',result['reason']);notice=self.adapter.sent[-1][1]
  self.assertIn('/answer',notice);self.assertNotIn('/choose',notice);self.assertIn('Question reference:',notice)
  self.assertEqual(self.ctx.state.get('inbound',{}),{});self.assertEqual(self.transport.calls,[])

 async def test_missing_native_identity_is_not_replaced_or_recorded(self):
  sent=[]
  async def capture(chat,text,**kwargs):sent.append((text,kwargs))
  self.adapter.send=capture
  e=event('/choose 1');e.message_id=None
  await self.plugin.handle_event(e);await asyncio.sleep(0)
  self.assertEqual(len(sent),1);self.assertIn('cannot verify',sent[0][0]);self.assertIsNone(sent[0][1]['reply_to'])
  self.assertEqual(self.ctx.state.get('inbound',{}),{});self.assertEqual(self.transport.calls,[])

 async def test_wrong_binding_and_missing_quote_do_not_trigger_validation_send(self):
  await self.plugin.handle_event(event('/choose',sender='other'))
  await self.plugin.handle_event(event('/choose',quote=None));await asyncio.sleep(0)
  self.assertEqual(self.adapter.sent,[]);self.assertEqual(self.transport.calls,[])

 async def test_notification_failure_never_creates_answer(self):
  self.adapter.ambiguous=True
  result=await self.plugin.handle_event(event('/choose'));await asyncio.sleep(0)
  self.assertEqual(result['action'],'skip');self.assertEqual(self.ctx.state.get('inbound',{}),{});self.assertEqual(self.transport.calls,[])
