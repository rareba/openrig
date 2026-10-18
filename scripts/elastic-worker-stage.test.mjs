import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
test('fixed stager refuses archive escapes, duplicate/link files and changed package plans before effects', () => {
 const program = String.raw`
import importlib.util,io,tarfile,tempfile,pathlib,sys
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('stage',${JSON.stringify(resolve('integrations/azure-elastic/stage-worker.py'))}); m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def archive(entries):
 b=io.BytesIO()
 with tarfile.open(fileobj=b,mode='w') as t:
  for name,kind in entries:
   e=tarfile.TarInfo(name);e.mode=0o644
   if kind=='link': e.type=tarfile.SYMTYPE;e.linkname='/etc/passwd';t.addfile(e)
   else: e.size=1;t.addfile(e,io.BytesIO(b'x'))
 b.seek(0);return b
for entries in [[('../escape','file')],[('/absolute','file')],[('openrig/a','file'),('openrig/a','file')],[('openrig/a','link')]]:
 with tempfile.TemporaryDirectory() as d:
  try:m.extract_archive(archive(entries),pathlib.Path(d));raise AssertionError('unsafe archive accepted')
  except m.StageRejected:pass
m.validate_apt_plan('Inst git (1.2 Ubuntu [amd64])\nConf git (1.2 Ubuntu [amd64])\n',[{'package':'git','version':'1.2','architecture':'amd64'}])
for plan in ['Remv git [1.2]','Inst evil (1.2 Ubuntu [amd64])','Inst git (9.9 Ubuntu [amd64])']:
 try:m.validate_apt_plan(plan,[{'package':'git','version':'1.2','architecture':'amd64'}]);raise AssertionError('unsafe plan accepted')
 except m.StageRejected:pass
assert m.tool_probe_argv('/usr/bin/tmux') == ['/usr/bin/tmux','-V']
assert '--no-remove' in m.apt_argv(['/private/fixture.deb'],pathlib.Path('/private/apt'),True)
assert '--no-download' not in m.apt_argv(['/private/fixture.deb'],pathlib.Path('/private/apt'),True)
assert '/private/fixture.deb' in m.apt_argv(['/private/fixture.deb'],pathlib.Path('/private/apt'),True)
`;
 assert.equal(execFileSync('python3', ['-c', program], { encoding: 'utf8' }), '');
});
test('strict downloader checks actual TLS1.3 IP SAN, digest, length and refuses redirects', context => {
 const python = ['python3', 'python3.14', 'python3.13', 'python3.12'].find(program => {
  try { return execFileSync(program, ['-c', 'import ssl; print(ssl.HAS_TLSv1_3)'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim() === 'True'; } catch { return false; }
 });
 if (!python) { context.skip('Python with TLS1.3 support is required for this fixture.'); return; }
 const program=String.raw`
import importlib.util,tempfile,pathlib,subprocess,ssl,http.server,threading,socket,hashlib,json,sys
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('stage',${JSON.stringify(resolve('integrations/azure-elastic/stage-worker.py'))});m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
with tempfile.TemporaryDirectory() as directory:
 root=pathlib.Path(directory);key=root/'key.pem';cert=root/'cert.pem'
 subprocess.run(['/usr/bin/openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=fixture','-addext','subjectAltName=IP:10.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 data=b'fixture-runtime';checksum=hashlib.sha256(data).hexdigest();mode=['ok']
 class Handler(http.server.BaseHTTPRequestHandler):
  def do_GET(self):
   self.send_response(302 if mode[0]=='redirect' else 200)
   if mode[0]=='redirect':self.send_header('Location','https://example.invalid/leak')
   self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
  def log_message(self,*args):pass
 class Server(http.server.HTTPServer):
  def handle_error(self,*args):
   if not isinstance(sys.exc_info()[1],ConnectionResetError):super().handle_error(*args)
 server=Server(('127.0.0.1',0),Handler)
 context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_3;context.load_cert_chain(str(cert),str(key));server.socket=context.wrap_socket(server.socket,server_side=True)
 thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
 original=socket.create_connection
 def routed(address,*args,**kwargs):
  assert address==('10.0.0.1',server.server_port)
  return original(('127.0.0.1',server.server_port),*args,**kwargs)
 socket.create_connection=routed
 pem=cert.read_text();a={'version':1,'url':'https://10.0.0.1:'+str(server.server_port)+'/api/elastic-artifacts/'+checksum,'privateIPv4':'10.0.0.1','archiveSha256':checksum,'archiveBytes':len(data),'caPem':pem,'caSha256':hashlib.sha256(pem.encode()).hexdigest(),'certificateSha256':hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()}
 try:
  output=root/'good.tar';m.download_artifact(a,output);assert output.read_bytes()==data
  for changed in [{'archiveBytes':len(data)+1},{'certificateSha256':'0'*64}]:
   try:m.download_artifact({**a,**changed},root/('bad'+str(len(list(root.iterdir())))));raise AssertionError('bad TLS/length accepted')
   except m.StageRejected:pass
  mode[0]='redirect'
  try:m.download_artifact(a,root/'redirect.tar');raise AssertionError('redirect accepted')
  except m.StageRejected:pass
  for changed in [{'url':a['url']+'?token=x'},{'privateIPv4':'127.0.0.1'},{'caSha256':'0'*64}]:
   try:m.validate_artifact({**a,**changed});raise AssertionError('bad authority accepted')
   except m.StageRejected:pass
 finally:server.shutdown();server.server_close();thread.join();socket.create_connection=original
`;
 assert.equal(execFileSync(python,['-c',program],{encoding:'utf8',timeout:15000}),'');
});
