"""Mapped native WhatsApp identities preserve the existing bound answer tuple."""
import copy
import importlib
import os
import pathlib
import re
import sys
import tempfile
import types
import unittest
from unittest import mock

from test_plugin import Adapter, Context, DELIVERY, Transport, event, module

LID = '100000000000001@lid'
PHONE = '15551234567@s.whatsapp.net'


class WhatsAppBindingTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.tmp = tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.env = mock.patch.dict(os.environ, {'HERMES_HOME':self.tmp.name});self.env.start();self.addCleanup(self.env.stop)
  directory=pathlib.Path(self.tmp.name)/'whatsapp/session';directory.mkdir(parents=True)
  (directory/'lid-mapping-15551234567.json').write_text('"100000000000001"')
  (directory/'lid-mapping-100000000000001_reverse.json').write_text('"15551234567"')
  try:
   importlib.import_module('gateway.whatsapp_identity')
  except ImportError:
   # Standalone plugin CI has no Hermes installation. Native-SDK runs use the
   # real helper and these operator-owned files; the unit substitute models its API.
   def participant(value):
    value=str(value or '')
    return value if re.fullmatch(r'[0-9]+@(s\.whatsapp\.net|lid)',value) else ''
   def canonical(value):
    bare=value.partition('@')[0]
    return '15551234567' if bare in ('15551234567','100000000000001') else bare
   native=types.ModuleType('gateway.whatsapp_identity');native.normalize_whatsapp_mention_jid=participant;native.canonical_whatsapp_identifier=canonical
   native.expand_whatsapp_aliases=lambda value: {'15551234567','100000000000001'} if value.partition('@')[0] in ('15551234567','100000000000001') else {value.partition('@')[0]}
   patched=mock.patch.dict(sys.modules,{'gateway':types.ModuleType('gateway'),'gateway.whatsapp_identity':native});patched.start();self.addCleanup(patched.stop)
  self.ctx=Context();self.transport=Transport();self.plugin=module.RelayPlugin(self.ctx,self.transport,now=lambda:10000);self.plugin.adapter=Adapter()
  self.delivery=copy.deepcopy(DELIVERY);self.delivery.update(chatId=LID,senderId=LID)
  self.ctx.state.set('outbound',{'original':{'storageKey':'original','delivery':self.delivery,'hostId':'origin','state':'acknowledged','messageIds':['platform-1'],'at':1}})

 async def test_mapped_sender_and_chat_reach_api_as_existing_bound_tuple(self):
  for chat,sender in ((LID,PHONE),(PHONE,LID),(PHONE,PHONE)):
   with self.subTest(chatChanged=chat!=LID,senderChanged=sender!=LID):
    self.ctx.state.set('inbound',{});self.transport.calls=[]
    e=event(sender=sender);e.source.chat_id=chat
    result=await self.plugin.handle_event(e)
    self.assertEqual(result['action'],'skip')
    records=list(self.ctx.state.get('inbound',{}).values());self.assertEqual(len(records),1)
    payload=records[0]['payload'];self.assertEqual((payload['profile'],payload['chatId'],payload['senderId']),('default',LID,LID))
    self.assertEqual(payload['replyToMessageId'],'platform-1');self.assertEqual(payload['messageId'],'inbound-1')
    self.assertEqual(records[0]['observedSource'],{'profile':'default','chatId':chat,'senderId':sender})
    identity=next(iter(self.ctx.state.get('inbound')));await self.plugin.flush_inbound(identity,records[0])
    self.assertTrue(any(op=='respond' and p['chatId']==LID and p['senderId']==LID for op,p in self.transport.calls))

 async def test_alias_retry_keeps_one_durable_native_message_identity(self):
  e=event(sender=PHONE);e.source.chat_id=LID
  await self.plugin.handle_event(e)
  retry=event(sender=LID);retry.source.chat_id=LID
  result=await self.plugin.handle_event(retry)
  self.assertIn('already has a durable',result['reason']);self.assertEqual(len(self.ctx.state.get('inbound')),1)
  record=next(iter(self.ctx.state.get('inbound').values()))
  self.assertEqual(record['observedSource']['senderId'],PHONE)
  self.assertIsNotNone(self.plugin.known_question('platform-1','whatsapp',('default',LID,PHONE)))

 async def test_wrong_principal_profile_and_non_numeric_domains_remain_refused(self):
  cases=[(LID,'15550000000@s.whatsapp.net','default'),('15550000000@s.whatsapp.net',PHONE,'default'),
         (LID,PHONE,'other'),(LID,'100000000000001@s.whatsapp.net','default'),
         (LID,'100000000000001@g.us','default'),(LID,'100000000000001@other','default')]
  for chat,sender,profile in cases:
   with self.subTest(chatChanged=chat!=LID,senderDomain=sender.partition('@')[2],profileChanged=profile!='default'):
    e=event(sender=sender);e.source.chat_id=chat;e.source.profile=profile
    e.raw_message={'senderAltId':LID,'chatId':chat,'senderId':sender,'messageId':e.message_id}
    result=await self.plugin.handle_event(e)
    self.assertEqual(result['action'],'skip');self.assertIn('profile_matches=',result['reason'])
    self.assertNotIn(LID,result['reason']);self.assertNotIn(PHONE,result['reason'])
  self.assertEqual(self.ctx.state.get('inbound',{}),{});self.assertFalse(any(op=='respond' for op,p in self.transport.calls))

 async def test_slack_and_teams_keep_exact_binding_checks(self):
  for channel in ('slack','teams'):
   item=copy.deepcopy(self.ctx.state.get('outbound')['original']);item['delivery']['channel']=channel;self.ctx.state.set('outbound',{'original':item})
   e=event(sender=PHONE);e.source.platform.value=channel;e.source.chat_id=LID
   self.assertEqual((await self.plugin.handle_event(e))['action'],'skip')
  self.assertEqual(self.ctx.state.get('inbound',{}),{});self.assertFalse(any(op=='respond' for op,p in self.transport.calls))

 async def test_missing_quote_and_untrusted_alt_id_cannot_authorize(self):
  e=event(sender=PHONE,quote=None);e.source.chat_id=LID
  self.assertIsNone(await self.plugin.handle_event(e));self.assertEqual(self.ctx.state.get('inbound',{}),{})
  e=event(sender='15550000000@s.whatsapp.net');e.source.chat_id=LID;e.raw_message={'senderAltId':PHONE}
  self.assertEqual((await self.plugin.handle_event(e))['action'],'skip');self.assertEqual(self.ctx.state.get('inbound',{}),{})

 async def test_colliding_quote_ids_are_ambiguous_even_for_same_binding(self):
  other=copy.deepcopy(self.ctx.state.get('outbound')['original']);other['delivery']['qitemId']='question-2';other['delivery']['deliveryId']='delivery-2'
  items=self.ctx.state.get('outbound');items['other']=other;self.ctx.state.set('outbound',items)
  e=event(sender=LID);e.source.chat_id=LID
  result=await self.plugin.handle_event(e)
  self.assertEqual(result['action'],'skip');self.assertIn('ambiguous',result['reason'].lower());self.assertEqual(self.ctx.state.get('inbound',{}),{})

 async def test_alias_equivalent_bound_questions_with_colliding_quote_are_ambiguous(self):
  other=copy.deepcopy(self.ctx.state.get('outbound')['original'])
  other['delivery'].update(chatId=PHONE,senderId=PHONE,qitemId='question-2',deliveryId='delivery-2')
  items=self.ctx.state.get('outbound');items['other']=other;self.ctx.state.set('outbound',items)
  e=event(sender=PHONE);e.source.chat_id=LID
  result=await self.plugin.handle_event(e)
  self.assertEqual(result['action'],'skip');self.assertIn('ambiguous',result['reason'].lower())
  self.assertEqual(self.ctx.state.get('inbound',{}),{})

 async def test_conflicting_native_mapping_cannot_extend_bound_pair(self):
  native=importlib.import_module('gateway.whatsapp_identity')
  directory=pathlib.Path(self.tmp.name)/'whatsapp/session'
  (directory/'lid-mapping-19999999999.json').write_text('"100000000000001"')
  third='19999999999@s.whatsapp.net'
  if hasattr(native, '__file__'):
   self.assertEqual(native.canonical_whatsapp_identifier(third),native.canonical_whatsapp_identifier(LID))
   e=event(sender=third);e.source.chat_id=LID
   result=await self.plugin.handle_event(e)
  else:
   with mock.patch.object(native,'expand_whatsapp_aliases',return_value={'15551234567','100000000000001','19999999999'}):
    e=event(sender=PHONE);e.source.chat_id=LID
    result=await self.plugin.handle_event(e)
  self.assertEqual(result['action'],'skip');self.assertEqual(self.ctx.state.get('inbound',{}),{})

 async def test_identity_helper_unavailable_fails_closed_for_aliases(self):
  with mock.patch.dict(sys.modules,{'gateway.whatsapp_identity':None}):
   e=event(sender=PHONE);e.source.chat_id=LID
   result=await self.plugin.handle_event(e)
  self.assertEqual(result['action'],'skip');self.assertEqual(self.ctx.state.get('inbound',{}),{})


if __name__=='__main__':unittest.main()
