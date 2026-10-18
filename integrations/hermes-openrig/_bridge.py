"""Optional outbound-only bridge to an existing, operator-configured loopback route."""
import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never let an existing local route nominate another destination.


class LocalWebhookBridge:
    def __init__(self, ctx):
        self.url = ctx.get_config('bridge_url', '')
        parsed = urllib.parse.urlsplit(self.url)
        try:
            loopback = ipaddress.ip_address(parsed.hostname or '').is_loopback
            _ = parsed.port  # Validate any explicit port.
        except ValueError:
            loopback = False
        if (parsed.scheme != 'http' or not loopback or parsed.username is not None
                or parsed.password is not None or parsed.fragment):
            raise ValueError('bridge_url must be a numeric loopback HTTP endpoint without credentials or fragment.')
        secret_file = ctx.get_config('bridge_secret_file', '')
        if not isinstance(secret_file, str) or not secret_file:
            raise ValueError('bridge_secret_file must name the existing local route secret.')
        self.secret_file = pathlib.Path(secret_file).expanduser()
        self.channels = ctx.get_config('bridge_channels', ['whatsapp'])
        if (not isinstance(self.channels, list) or not 1 <= len(self.channels) <= 3
                or len(set(self.channels)) != len(self.channels)
                or any(channel not in ('whatsapp', 'slack', 'teams') for channel in self.channels)):
            raise ValueError('bridge_channels must contain one to three distinct supported channels.')

    async def post(self, event):
        def request():
            secret = self.secret_file.read_text().strip()
            if not secret:
                raise ValueError('The configured local route secret is empty.')
            body = json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
            identity = event['deliveryId']
            stamp = str(int(time.time()))
            signed = (identity + '.' + stamp + '.').encode('utf-8') + body
            signature = base64.b64encode(hmac.new(secret.encode('utf-8'), signed, hashlib.sha256).digest()).decode('ascii')
            req = urllib.request.Request(self.url, data=body, method='POST', headers={
                'Content-Type': 'application/json', 'webhook-id': identity,
                'webhook-timestamp': stamp, 'webhook-signature': 'v1,' + signature})
            # No ambient proxies or redirects: this optional bridge stays on loopback.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
            try:
                with opener.open(req, timeout=5) as response:
                    raw = response.read(65537)
                    if len(raw) > 65536:
                        raise ValueError('Local relay acknowledgment is too large.')
                    ack = json.loads(raw)
            except urllib.error.HTTPError as error:
                error.close()
                raise
            expected = {'schema': 'openrig.human-relay.v1', 'status': 'accepted',
                        'deliveryId': identity, 'fingerprint': event['fingerprint']}
            if ack != expected:
                raise ValueError('Local relay acknowledgment did not match the immutable event.')
            return ack
        return await asyncio.wait_for(asyncio.to_thread(request), timeout=5)
