"""The optional bridge signs only a configured loopback HTTP destination."""
import asyncio
import base64
import copy
import hashlib
import hmac
import http.server
import json
import pathlib
import tempfile
import threading
import unittest
from test_plugin import Context, Transport, DELIVERY, module
from test_webhooks import envelope

class BridgeTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.home=tempfile.TemporaryDirectory();self.secret=pathlib.Path(self.home.name,'secret');self.secret.write_text('synthetic-secret')
  self.ctx=Context();self.transport=Transport();self.requests=[];self.drop_ack=False;self.server_response=None
  loop=asyncio.get_running_loop();test=self
  class Handler(http.server.BaseHTTPRequestHandler):
   def log_message(self,*args):pass
   def do_POST(self):
    raw=self.rfile.read(int(self.headers['Content-Length']));identity=self.headers['webhook-id'];stamp=self.headers['webhook-timestamp']
    expected='v1,'+base64.b64encode(hmac.new(b'synthetic-secret',(identity+'.'+stamp+'.').encode()+raw,hashlib.sha256).digest()).decode()
    if not hmac.compare_digest(self.headers['webhook-signature'],expected):self.send_error(403);return
    event=json.loads(raw);test.requests.append((self.path,raw,event))
    answer=asyncio.run_coroutine_threadsafe(test.plugin.handle_webhook(route_name='openrig-human-relay',profile='default',payload=event,delivery_id=identity),loop).result(timeout=3)
    if test.drop_ack:test.drop_ack=False;self.close_connection=True;return
    if test.server_response is not None:answer=test.server_response
    content=json.dumps(answer['body']).encode();self.send_response(answer['status'])
    if answer.get('location'):self.send_header('Location',answer['location'])
    self.end_headers();self.wfile.write(content)
  self.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
  self.url=f'http://127.0.0.1:{self.server.server_port}/hooks/openrig-human-relay'
  self.config={'delivery_mode':'webhook-bridge','bridge_url':self.url,'bridge_secret_file':str(self.secret)}
  original=self.ctx.get_config;self.ctx.get_config=lambda k,d=None:self.config.get(k,original(k,d))
  self.plugin=module.RelayPlugin(self.ctx,self.transport)
  self.original=self.transport.post;self.accepted=False;self.lose_native_ack=False
  async def post(op,p):
   if op=='webhook-pull':
    self.transport.calls.append((op,copy.deepcopy(p)))
    return {'events':[] if self.accepted else [envelope()]}
   if op=='webhook-accepted':
    self.transport.calls.append((op,copy.deepcopy(p)))
    if self.lose_native_ack:self.lose_native_ack=False;raise OSError('lost native acceptance acknowledgment')
    self.accepted=True;return {'state':'accepted'}
   return await self.original(op,p)
  self.transport.post=post
 async def asyncTearDown(self):
  self.plugin.stop();await asyncio.to_thread(self.server.shutdown);self.server.server_close();self.thread.join();self.home.cleanup()
 async def test_signed_canonical_loopback_post_and_native_acceptance(self):
  await self.plugin.poll_once()
  self.assertEqual(len(self.requests),1);path,raw,event=self.requests[0]
  self.assertEqual(path,'/hooks/openrig-human-relay');self.assertEqual(raw,json.dumps(event,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode())
  accepted=next(p for op,p in self.transport.calls if op=='webhook-accepted')
  self.assertEqual(accepted,{'hostId':'origin','connectorId':'hermes','profile':'default','channel':'whatsapp','chatId':'chat','senderId':'person','deliveryId':DELIVERY['deliveryId'],'attemptId':'attempt-1','fingerprint':envelope()['fingerprint']})
  self.assertFalse(any(op=='pull' for op,p in self.transport.calls))
 async def test_lost_http_ack_reposts_same_event_and_stores_only_once(self):
  self.drop_ack=True;await self.plugin.poll_once();self.assertFalse(self.accepted)
  await self.plugin.poll_once();self.assertTrue(self.accepted);self.assertEqual(len(self.requests),2)
  self.assertEqual(self.requests[0][1],self.requests[1][1]);self.assertEqual(len(self.ctx.state.get('outbound')),1)
 async def test_lost_native_ack_reuses_callback_stable_acceptance(self):
  self.lose_native_ack=True;await self.plugin.poll_once();self.assertFalse(self.accepted)
  await self.plugin.poll_once();self.assertTrue(self.accepted);self.assertEqual(len(self.ctx.state.get('outbound')),1)
 async def test_storage_failure_or_wrong_ack_never_marks_native_accepted(self):
  original=self.ctx.state.set
  def full(key,value):
   if key=='outbound' and value:raise OSError('full')
   return original(key,value)
  self.ctx.state.set=full;await self.plugin.poll_once();self.assertFalse(self.accepted)
  self.ctx.state.set=original;self.server_response={'status':200,'body':{'status':'accepted'}}
  await self.plugin.poll_once();self.assertFalse(self.accepted)
 async def test_redirect_does_not_follow_another_endpoint(self):
  self.server_response={'status':302,'body':{},'location':self.url+'/elsewhere'}
  await self.plugin.poll_once();self.assertFalse(self.accepted);self.assertEqual(len(self.requests),1)
 async def test_nonloopback_or_nonhttp_bridge_config_is_rejected(self):
  for url in ('https://127.0.0.1/hook','http://example.com/hook','http://127.0.0.1@evil.test/hook','http://127.0.0.1/hook#fragment'):
   self.config['bridge_url']=url
   with self.assertRaises(ValueError):module.RelayPlugin(self.ctx,self.transport)
 async def test_host_channel_rotation_is_bounded_and_adapter_independent(self):
  self.config.update(origin_hosts=['one','two'],bridge_channels=['whatsapp','slack','teams'])
  plugin=module.RelayPlugin(self.ctx,self.transport)
  for _ in range(6):await plugin.poll_once()
  pulls=[p for op,p in self.transport.calls if op=='webhook-pull']
  self.assertEqual({(p['hostId'],p['channel']) for p in pulls},{(h,c) for h in ('one','two') for c in ('whatsapp','slack','teams')})
  self.assertTrue(all(p['limit']==2 for p in pulls));plugin.stop()
