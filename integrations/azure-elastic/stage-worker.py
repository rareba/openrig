#!/usr/bin/python3
"""Pinned trusted-image stager. No target repository scripts or online resolution run here."""
import base64
import hashlib
import http.client
import io
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import ssl
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.parse import urlsplit

MAX_ARCHIVE = 1536 * 1024 * 1024
MAX_UNPACKED = 3 * 1024 * 1024 * 1024
STAGE_ROOT = Path('/var/lib/openrig-elastic-stage')
ROOTS = {'openrig': Path('/opt/openrig'), 'pi': Path('/opt/pi'), 'node': Path('/opt/node')}
class StageRejected(Exception):
    pass

def require(condition):
    if not condition:
        raise StageRejected('worker_stage_rejected')

def digest(data):
    return hashlib.sha256(data).hexdigest()

def strict_keys(value, keys, optional=()):
    require(isinstance(value, dict) and set(keys) <= set(value) and set(value) <= set(keys) | set(optional))

def trusted_path(path, directory=False, private=False):
    path = Path(path)
    require(path.is_absolute() and str(path.resolve()) == str(path))
    for parent in [path, *path.parents]:
        s = parent.lstat()
        require(s.st_uid == 0 and not s.st_mode & 0o022 and not stat.S_ISLNK(s.st_mode))
    s = path.lstat()
    require(stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode) and s.st_nlink == 1)
    if private:
        require(s.st_mode & 0o077 == 0)

def atomic(path, data, mode=0o600):
    trusted_path(path.parent, directory=True)
    if path.exists():
        trusted_path(path)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.stage-')
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        d = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(name):
            os.unlink(name)

def validate_artifact(a):
    strict_keys(a, ['version', 'url', 'privateIPv4', 'archiveSha256', 'archiveBytes', 'caPem', 'caSha256'], ['certificateSha256'])
    u = urlsplit(a['url'])
    ip = ipaddress.IPv4Address(a['privateIPv4'])
    private = any(ip in ipaddress.IPv4Network(n) for n in ['10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'])
    require(a['version'] == 1 and private and u.scheme == 'https' and u.hostname == str(ip) and 1024 <= (u.port or 443) <= 65535 and not u.username and not u.password and not u.query and not u.fragment)
    require(re.fullmatch(r'[a-f0-9]{64}', a['archiveSha256']) and u.path == '/api/elastic-artifacts/' + a['archiveSha256'])
    require(type(a['archiveBytes']) is int and 0 < a['archiveBytes'] <= MAX_ARCHIVE)
    require(isinstance(a['caPem'], str) and len(a['caPem']) <= 32768 and digest(a['caPem'].encode()) == a['caSha256'])
    if 'certificateSha256' in a:
        require(re.fullmatch(r'[a-f0-9]{64}', a['certificateSha256']))
    return u

def download_artifact(a, output):
    u = validate_artifact(a)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cadata=a['caPem'])
    connection = http.client.HTTPSConnection(u.hostname, u.port, timeout=10, context=context)
    started = time.monotonic()
    try:
        connection.connect()
        if 'certificateSha256' in a:
            require(digest(connection.sock.getpeercert(binary_form=True)) == a['certificateSha256'])
        connection.request('GET', u.path, headers={'Accept-Encoding': 'identity', 'Connection': 'close'})
        response = connection.getresponse()
        require(response.status == 200 and response.getheader('Content-Encoding') in (None, 'identity') and response.getheader('Transfer-Encoding') is None)
        require(response.getheader('Content-Length') == str(a['archiveBytes']))
        total = 0
        checksum = hashlib.sha256()
        with open(output, 'xb') as stream:
            os.chmod(output, 0o600)
            while True:
                require(time.monotonic() - started < 180)
                part = response.read(min(65536, a['archiveBytes'] + 1 - total))
                if not part:
                    break
                total += len(part)
                require(total <= a['archiveBytes'])
                checksum.update(part)
                stream.write(part)
            require(total == a['archiveBytes'] and checksum.hexdigest() == a['archiveSha256'])
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        connection.close()

def extract_archive(source, destination):
    """Never invoke tar extraction: copy only bounded approved ordinary entries."""
    names = set()
    total = 0
    with tarfile.open(fileobj=source, mode='r:') as archive:
        for entry in archive:
            name = entry.name.rstrip('/')
            path = PurePosixPath(name)
            require(name and str(path) == name and not path.is_absolute() and '..' not in path.parts and name not in names and len(name) <= 4096)
            names.add(name)
            require(len(names) <= 250000 and path.parts[0] in {'openrig', 'pi', 'node', 'os-packages', 'provenance.json'})
            require(entry.isdir() or entry.isfile())
            require(entry.mode & ~0o777 == 0 and entry.mode & 0o022 == 0)
            require(entry.uid == 0 and entry.gid == 0)
            target = destination.joinpath(*path.parts)
            if entry.isdir():
                target.mkdir(mode=0o755, parents=True, exist_ok=True)
                continue
            require(0 <= entry.size <= 512 * 1024 * 1024)
            total += entry.size
            require(total <= MAX_UNPACKED)
            target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            with archive.extractfile(entry) as incoming, open(target, 'xb') as outgoing:
                remaining = entry.size
                while remaining:
                    chunk = incoming.read(min(65536, remaining))
                    require(bool(chunk))
                    outgoing.write(chunk)
                    remaining -= len(chunk)
            os.chmod(target, 0o755 if entry.mode & 0o111 else 0o644)
    require({'openrig', 'pi', 'node', 'os-packages', 'provenance.json'} <= {PurePosixPath(n).parts[0] for n in names})

def run(argv, timeout=30, env=None):
    result = subprocess.run(argv, env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'HOME': '/root', 'LANG': 'C', 'LC_ALL': 'C', 'DEBIAN_FRONTEND': 'noninteractive', **(env or {})}, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    require(result.returncode == 0 and len(result.stdout) <= 8 * 1024 * 1024 and len(result.stderr) <= 1024 * 1024)
    return result.stdout.decode('utf8', errors='strict')

def apt_argv(files, root, simulate):
    return ['/usr/bin/apt-get', '--simulate' if simulate else '--assume-yes', '--no-remove', '--no-install-recommends', '-o', 'Dir::Etc::sourcelist=/dev/null', '-o', 'Dir::Etc::sourceparts=' + str(root / 'sources'), '-o', 'Dir::State::lists=' + str(root / 'lists'), '-o', 'APT::Get::allow-Downgrades=false', '-o', 'APT::Get::allow-Change-Held-Packages=false', 'install', *files]

def validate_apt_plan(output, packages):
    expected = {p['package']: p['version'] for p in packages}
    for line in output.splitlines():
        require(not line.startswith(('Remv ', 'Purg ')))
        if line.startswith(('Inst ', 'Conf ')):
            match = re.match(r'^(?:Inst|Conf) ([A-Za-z0-9+.-]+)(?::(?:amd64|all))?(?: \[[^\]]+\])? \(([^ ]+)', line)
            require(match is not None and expected.get(match[1]) == match[2])

def tool_probe_argv(path):
    return [path, '-V' if path == '/usr/bin/tmux' else '--version']

def install_packages(recipe, unpacked, stage):
    strict_keys(recipe, ['distribution', 'release', 'architecture', 'basePackages', 'packages', 'finalTools', 'signedMetadata'], ['baseInventorySha256'])
    require(recipe['distribution'] == 'ubuntu' and recipe['release'] == '24.04' and recipe['architecture'] == 'amd64')
    release = dict(line.split('=', 1) for line in Path('/etc/os-release').read_text().splitlines() if '=' in line)
    require(release.get('ID', '').strip('"') == recipe['distribution'] and release.get('VERSION_ID', '').strip('"') == recipe['release'])
    require(run(['/usr/bin/dpkg', '--print-architecture']).strip() == recipe['architecture'])
    require(not run(['/usr/bin/dpkg', '--audit']).strip())
    inventory = run(['/usr/bin/dpkg-query', '--show', '--showformat=${Package}\t${Version}\t${db:Status-Status}\n'])
    base = {}
    for line in inventory.splitlines():
        package, version, status = line.split('\t')
        require(status in ['installed', 'config-files'])
        if status == 'installed':
            base[package] = version
    if 'baseInventorySha256' in recipe:
        require(digest(('\n'.join(sorted(inventory.splitlines())) + '\n').encode()) == recipe['baseInventorySha256'])
    require(isinstance(recipe['basePackages'], dict) and 1 <= len(recipe['basePackages']) <= 64)
    for package, versions in recipe['basePackages'].items():
        require(isinstance(versions, list) and 1 <= len(versions) <= 16 and base.get(package) in versions)
    packages = recipe['packages']
    require(isinstance(packages, list) and 1 <= len(packages) <= 256 and len({p['package'] for p in packages}) == len(packages))
    files = []
    for p in packages:
        strict_keys(p, ['file', 'package', 'version', 'architecture', 'sha256', 'bytes'])
        require(re.fullmatch(r'[A-Za-z0-9_.+%~-]+\.deb', p['file']) and re.fullmatch(r'[a-z0-9][a-z0-9+.-]{0,100}', p['package']) and p['architecture'] in ['amd64', 'all'])
        path = unpacked / 'os-packages' / p['file']
        require(path.is_file() and path.stat().st_size == p['bytes'] and digest(path.read_bytes()) == p['sha256'])
        require(run(['/usr/bin/dpkg-deb', '--show', '--showformat=${Package}\t${Version}\t${Architecture}', str(path)]).strip() == '\t'.join([p['package'], p['version'], p['architecture']]))
        if p['package'] in base:
            run(['/usr/bin/dpkg', '--compare-versions', base[p['package']], 'le', p['version']])
        files.append(str(path))
    apt = stage / 'apt'
    for directory in [apt, apt / 'sources', apt / 'lists']:
        directory.mkdir(mode=0o700, exist_ok=True)
    simulation = run(apt_argv(files, apt, True), timeout=60)
    validate_apt_plan(simulation, packages)
    # Empty sources/list paths leave only the explicit verified local-file candidates.
    run(apt_argv(files, apt, False), timeout=300)
    require(not run(['/usr/bin/dpkg', '--audit']).strip())
    for p in packages:
        require(run(['/usr/bin/dpkg-query', '--show', '--showformat=${Version}', p['package']]).strip() == p['version'])
    require({tool['path'] for tool in recipe['finalTools']} == {'/usr/bin/git', '/usr/bin/gh', '/usr/bin/tmux', '/usr/bin/bash', '/usr/bin/prlimit', '/usr/bin/python3'})
    for tool in recipe['finalTools']:
        strict_keys(tool, ['path', 'sha256'])
        require(tool['path'] in ['/usr/bin/git', '/usr/bin/gh', '/usr/bin/tmux', '/usr/bin/bash', '/usr/bin/prlimit', '/usr/bin/python3'])
        target = Path(tool['path']).resolve()
        trusted_path(target)
        require(digest(target.read_bytes()) == tool['sha256'])
        run(tool_probe_argv(tool['path']))

def validate_manifest(raw, expected):
    require(isinstance(raw, str) and len(raw.encode()) <= 65536 and digest(raw.encode()) == expected)
    m = json.loads(raw)
    linux = m['linux']
    require(m['controlDirectory'] == '/var/lib/openrig-elastic-control' and linux['sourceRoot'] == '/opt/openrig' and linux['piRoot'] == '/opt/pi' and linux['node'] == '/opt/node/bin/node')
    require(linux['daemon'] == '/opt/openrig/packages/daemon/dist/index.js' and linux['helper'] == '/opt/openrig/packages/daemon/dist/adapters/elastic-worker-helper.js' and linux['identity'] == '/etc/openrig-elastic/instance.json' and linux['guard'] == '/etc/openrig-elastic/guard.json')
    required_pins = ['/opt/openrig', '/opt/pi', '/opt/node/bin/node', '/opt/node/bin/rig', '/opt/node/bin/gh', '/usr/bin/git', '/usr/bin/gh', '/usr/bin/tmux', '/usr/bin/bash', '/usr/bin/prlimit', linux['upload']['caFile']]
    for required in required_pins:
        require(any(pin['path'] == required or pin['kind'] == 'tree' and required.startswith(pin['path'] + '/') for pin in m['pins']))

    return m

def main():
    require(sys.platform == 'linux' and os.geteuid() == 0 and len(sys.argv) == 1)
    encoded = os.environ.get('OPENRIG_REQUEST', '')
    require(len(encoded) <= 256 * 1024)
    request = json.loads(base64.b64decode(encoded, validate=True))
    strict_keys(request, ['version', 'operation', 'instance', 'payload', 'requestDigest'])
    require(request['version'] == 1 and request['operation'] == 'stage')
    p = request['payload']
    strict_keys(p, ['artifact', 'manifestRaw', 'manifestDigest', 'packageRecipe', 'checkpointCaPem', 'preparationPlan'])
    m = validate_manifest(p['manifestRaw'], p['manifestDigest'])
    require(request['instance']['manifestDigest'] == p['manifestDigest'] and request['instance']['resourceId'] == m['resourceId'] and request['instance']['imageReference'] == m['linux']['imageReference'])
    if not STAGE_ROOT.exists():
        STAGE_ROOT.mkdir(mode=0o700)
    trusted_path(STAGE_ROOT, directory=True, private=True)
    record_path = STAGE_ROOT / 'stage.json'
    config_digest = digest(json.dumps(request, sort_keys=True, separators=(',', ':')).encode())
    if record_path.exists():
        trusted_path(record_path, private=True)
        previous = json.loads(record_path.read_text())
        require(previous['configurationDigest'] == config_digest)
        if previous['phase'] == 'installed':
            verify_pins()
            print(json.dumps({'version': 1, 'installed': True, 'manifestDigest': p['manifestDigest']}))
            return
        # Package/copy/installer timeout is partial state. Never blindly rerun privileged changes.
        raise StageRejected('worker_stage_partial_requires_reconciliation')
    for destination in ROOTS.values():
        require(not destination.exists())
        trusted_path(destination.parent, directory=True)
    require(not Path(m['linux']['database']).exists())
    atomic(record_path, json.dumps({'configurationDigest': config_digest, 'phase': 'downloading'}).encode())
    work = Path(tempfile.mkdtemp(dir=STAGE_ROOT, prefix='artifact-'))
    archive = work / 'bundle.tar'
    download_artifact(p['artifact'], archive)
    unpacked = work / 'unpacked'
    unpacked.mkdir(mode=0o700)
    with open(archive, 'rb') as incoming:
        extract_archive(incoming, unpacked)
    provenance = json.loads((unpacked / 'provenance.json').read_text())
    require(provenance['piVersion'] == '1.0.4' and provenance['piEntry'] == m['linux']['pi'] and provenance['packageRecipeDigest'] == digest(json.dumps(p['packageRecipe'], separators=(',', ':'), ensure_ascii=False).encode()))
    atomic(record_path, json.dumps({'configurationDigest': config_digest, 'phase': 'packages'}).encode())
    install_packages(p['packageRecipe'], unpacked, work)
    atomic(record_path, json.dumps({'configurationDigest': config_digest, 'phase': 'copying'}).encode())
    for source, destination in ROOTS.items():
        shutil.copytree(unpacked / source, destination, copy_function=shutil.copyfile)
        for path in [destination, *destination.rglob('*')]:
            original = unpacked / source / path.relative_to(destination)
            os.chmod(path, 0o755 if path.is_dir() or original.stat().st_mode & 0o111 else 0o644)
            os.chown(path, 0, 0)
    control = Path('/etc/openrig-elastic')
    if not control.exists():
        control.mkdir(mode=0o755)
    trusted_path(control, directory=True)
    ca = Path(m['linux']['upload']['caFile'])
    require(ca == control / 'controller-ca.pem' and len(p['checkpointCaPem']) <= 32768)
    atomic(ca, p['checkpointCaPem'].encode(), 0o644)
    for path, data, mode in [(control / 'manifest.json', p['manifestRaw'].encode(), 0o644), (control / 'manifest.sha256', (p['manifestDigest'] + '\n').encode(), 0o644), (control / 'instance.json', json.dumps(request['instance'], separators=(',', ':')).encode(), 0o600)]:
        require(not path.exists())
        atomic(path, data, mode)
    atomic(control / 'preparation.json', json.dumps(p['preparationPlan'], separators=(',', ':')).encode(), 0o644)
    verify_pins()
    atomic(record_path, json.dumps({'configurationDigest': config_digest, 'phase': 'installing'}).encode())
    require(run(['/opt/node/bin/node', '/opt/openrig/packages/daemon/dist/adapters/elastic-worker-install.js'], timeout=180).strip() == 'worker_image_installed')
    atomic(record_path, json.dumps({'configurationDigest': config_digest, 'phase': 'installed'}).encode())
    shutil.rmtree(work)
    print(json.dumps({'version': 1, 'installed': True, 'manifestDigest': p['manifestDigest']}))

def verify_pins():
    script = "import{readFileSync}from'node:fs';import{parseLinuxManifest,pinDigest}from'/opt/openrig/packages/daemon/dist/adapters/linux-privileged-worker-boundary.js';const m=parseLinuxManifest(readFileSync('/etc/openrig-elastic/manifest.json','utf8'));for(const p of m.pins)if(pinDigest(p.path,p.kind)!==p.sha256)throw Error();"
    run(['/opt/node/bin/node', '--input-type=module', '-e', script])

if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.stderr.write('worker_stage_rejected\n')
        sys.exit(1)
