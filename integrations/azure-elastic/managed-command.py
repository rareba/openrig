#!/usr/bin/python3
"""Fixed managed RunCommand bridge. Input is an encrypted typed helper request, never shell."""
import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile

ALLOWED = {'prepare', 'enroll', 'github-credential-install'}
def require(value):
    if not value:
        raise RuntimeError()

def trusted(path, private=False):
    require(path.is_absolute() and path.resolve() == path)
    for p in [path, *path.parents]:
        s = p.lstat()
        require(s.st_uid == 0 and not s.st_mode & 0o022 and not stat.S_ISLNK(s.st_mode))
    s = path.stat()
    require(s.st_nlink == 1 and stat.S_ISREG(s.st_mode))
    if private:
        require(s.st_mode & 0o077 == 0)

def main():
    require(sys.platform == 'linux' and os.geteuid() == 0 and len(sys.argv) == 1)
    encoded = os.environ.get('OPENRIG_REQUEST', '')
    require(0 < len(encoded) <= 256 * 1024)
    request = json.loads(base64.b64decode(encoded, validate=True))
    require(set(request) == {'version', 'operation', 'instance', 'payload', 'requestDigest'} and request['version'] == 1 and request['operation'] in ALLOWED)
    manifest_path = Path('/etc/openrig-elastic/manifest.json')
    trusted(manifest_path)
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    require(hashlib.sha256(raw).hexdigest() == request['instance']['manifestDigest'])
    require(manifest['controlDirectory'] == '/var/lib/openrig-elastic-control' and manifest['linux']['identity'] == '/etc/openrig-elastic/instance.json')
    instance_path = Path('/etc/openrig-elastic/instance.json')
    trusted(instance_path, private=True)
    require(json.loads(instance_path.read_text()) == request['instance'])
    path = Path('/var/lib/openrig-elastic-control/request.json')
    parent = path.parent
    require(parent.resolve() == parent and parent.stat().st_uid == 0 and parent.stat().st_mode & 0o077 == 0)
    require(not (parent / 'operation.lock').exists())
    if path.exists():
        trusted(path, private=True)
    data = json.dumps(request['payload'], separators=(',', ':')).encode()
    require(len(data) <= 128 * 1024)
    fd, temporary = tempfile.mkstemp(dir=parent, prefix='.request-')
    with os.fdopen(fd, 'wb') as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    result = subprocess.run(['/opt/node/bin/node', '/opt/openrig/packages/daemon/dist/adapters/elastic-worker-helper.js', request['operation']], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'HOME': '/root'}, timeout=170)
    require(result.returncode == 0 and 0 < len(result.stdout) <= 4096)
    sys.stdout.buffer.write(result.stdout)

if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.stderr.write('worker_bootstrap_rejected\n')
        sys.exit(1)
