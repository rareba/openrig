# Linux elastic worker V1

This is an opt-in worker image/helper, not a controller or a deployment command. Task5 owns
ARM provisioning, trusted identity installation, upload reservation/server, independent
archive verification, receipt issuance, intake and human-return integration. A successful
build or these tests do not establish real Linux/provider readiness.

## Build and stage

Build with the pinned lockfile and Node 22/24: `npm ci`, then `npm run build:package`.
Stage the built checkout and its complete runtime dependency tree at `/opt/openrig`, Pi
**@earendil-works/pi-coding-agent 1.0.4** with all dependencies at `/opt/pi`, and the exact
Node executable at `/opt/node/bin/node`. Paths in the example are configurable in the
stable pool manifest. Installed trees must contain only canonical directories and ordinary
files, owned by root with no group/other write bits. Materialize package-manager symlinks
into their actual files during trusted image creation; symlinks/hardlinks in a pinned tree
are refused. Pin the complete source/dependency roots, not just JS entry files. The image
identity and root installation are the external trust anchor; the helper cannot prove its
own authenticity after it starts.

Stage the private TLS CA at `/etc/openrig-elastic/controller-ca.pem`, mode 0644, root-owned.
Create `/etc/openrig-elastic` mode 0755 root-owned. Customize `manifest.example.json` with
an exact immutable image version, actual paths, finite native session names, dedicated UID,
and private TLS IP/port. `linux.pi` must point to the real Pi Node entry file in that package
(use its pinned package `bin` field); do not point it at a shell wrapper. `stateRoot` must be
`home/state/pi`. Every writable workspace, cache, HOME, database and tool temp directory
must lie below `ownedRoots`; standard paths outside the inventory become read-only or
inaccessible in the unit. `/tmp` and `/var/tmp` bind to persistent inventoried `temp`.
`TMUX_TMPDIR` is pinned to that same canonical host-visible path. Only its exact
`tmux-<uid>/default` Unix listener can be represented as managed IPC: the live proof binds
its owner/device/inode/mode, private parent directory, `/proc/net/unix` listening kernel
inode, and an fd held by the pinned tmux process in the exact service cgroup with matching
`TMUX_TMPDIR` environment. No `/tmp` path alias, alternate socket, special file or whole
scratch directory is exempted. Ordinary temp files remain business custody. Quiesce records
that IPC identity before sealing; after twice proving whole-unit/UID absence it requires
that endpoint to be absent and retires only the exact same dead filesystem socket, if left
behind. The final archive retains the evidence/disposition in its audit. Replaced or still
live endpoints retain the VM.

On the staged Linux image, run as root with a clean environment:

```
/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin /opt/node/bin/node /opt/openrig/integrations/azure-elastic/pin-manifest.mjs /root/manifest.input.json
/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin /opt/node/bin/node /opt/openrig/packages/daemon/dist/adapters/elastic-worker-install.js
```

The installer creates the private control directory and an empty, fully migrated native
SQLite database. It refuses an existing database. It never starts the unit or enrolls a
worker. The manifest and its SHA-256 file become stable pool bytes; store that same digest
in the controller's bootstrap configuration. Do not rewrite them per VM.

Before `prepare`, the authenticated controller must independently resolve the exact VMSS
instance, its ARM vmId, exact image reference and its actual NIC private IPv4. It installs
`linux.identity` as a root-owned 0600 regular file with this exact object:

```
{"version":1,"resourceId":"<manifest resourceId>","imageReference":"<manifest linux.imageReference>","vmId":"<ARM UUID>","manifestDigest":"<stable manifest SHA256>","fleetEpoch":"<controller epoch>","operationId":"<original enrollment operation>","privateIPv4":"<exact ARM NIC address>"}
```

No HTTP self-report is sufficient. The helper also checks that private address on the
local NIC and derives workerId with `azureWorkerId(resourceId, vmId)`. The identity producer
must use authenticated terminal-success Run Command and the exact current ARM incarnation.
The worker has no fleet bearer and no controller account.

## Root dispatcher

Run outside `openrig-elastic.service`, never with sudo rights granted to its UID. Stage one
minified JSON object at `<controlDirectory>/request.json`, root-owned 0600, no symlinks or
hardlinks, max 128 KiB; all ancestors must be root-owned and non-writable to other users.
Final lifecycle operations refuse an existing operation lock; enrollment uses its own lock. Stale locks require trusted reconciliation.
Invoke exactly one fixed operation with clean environment:

```
/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin /opt/node/bin/node /opt/openrig/packages/daemon/dist/adapters/elastic-worker-helper.js prepare
```

Operations and exact request types are exported in `elastic-worker-helper.ts`:

- `prepare`: `{binding: EnrollmentBinding, controllerPublicKeyPem}`. Returns the bounded worker public key.
- `enroll`: `{binding, materialEnvelope}`. The existing separately encrypted Pi auth/settings/models codec applies. Returns only the encrypted bounded worker bearer. Creates/verifies locked no-login UID, per-seat files, root unit and root restart guard, then starts the foreground daemon. A retry cannot overwrite a running seat.
- `begin-drain`: `{identity, operationId}`. Native custody transaction, no removal authority.
- `quiesce`: `{binding: FinalBinding}`. Fresh native idle/census and clean inventory, durable native/root seal, whole-unit stop and twice-confirmed zero UID/cgroup processes. Shutdown mutations are preserved. This is **awaiting custody**, not removable.
- `final-checkpoint-export`: `{binding, capabilityEnvelope?}`. Without capability, creates the immutable encrypted checkpoint and returns its IDs/hash/size. With capability, retries the same ciphertext upload. `checkpoint-export` is an alias for this post-stop operation, not a live deletion shortcut.
- `final-checkpoint-ack`: `{binding, receipt: SignedCustodyReceipt}`. Verifies controller RSA-PSS signature, exact archive binding, native obligations, stopped UID/cgroup and unchanged final files/full SQLite logical content, then returns the removable seal. `checkpoint-ack` is an alias.
- `seal-status`: `{binding}`. Returns null awaiting custody, otherwise revalidates the saved exact signed receipt and stopped data before returning the seal.
- `start-condition`: unit-only, no request, denies pending/missing/sealed guard; startup additionally verifies native enrolled identity. No unseal operation exists.

Errors are generic; stdout is at most 4096 bytes and never carries archive bytes or secrets.
The legacy `AzureWorkerBootstrap.seal` now always refuses: its pre-stop manifest acknowledgment
cannot prove final saved custody.

## Task5 upload and custody wire contract

`FinalBinding` is `{workerId,vmId,manifestDigest,fleetEpoch,drainGeneration:number,removalOperationId}`.
CheckpointId is SHA256 of JSON `{binding,manifestDigest:<archive manifest digest>}`. All hashing
uses the exported serializers and original field order; consumers should use these codecs.

The controller reserves `{version:1,binding,checkpointId,token,expiresAt,maxBytes}`. Token is
32–256 base64url characters, expiry at most one hour ahead, bytes within the pinned ceiling.
Encrypt that JSON with `sealCheckpointCapability` and the prepared worker public key, using
the **original EnrollmentBinding**. The helper decrypts `capabilityEnvelope` using the saved
worker key and separately checks the final operation/checkpoint binding. Never submit a
plaintext capability through Run Command or put it in an argv/query string.

The client PUTs the ciphertext to exactly `linux.upload.origin + routePrefix + checkpointId`.
V1 origin hostname must equal the pinned RFC1918 IPv4; certificate SAN must match that IP,
TLS >=1.3, explicit pinned CA, no redirect. Authorization is a Bearer header containing only
the scoped capability. `x-openrig-checkpoint` is base64 JSON of
`{version,binding,checkpointId,manifestDigest,ciphertextSha256,ciphertextBytes,envelope}`.
The body is only the immutable AES-GCM ciphertext. The envelope has
`{algorithm,wrappedKey,iv,tag,aad,controllerKeyDigest}`; RSA-OAEP-SHA256 wraps a fresh 32-byte
AES key, IV is 12 bytes and tag 16 bytes. Exact AAD JSON binds version, final identity,
checkpointId, manifestDigest and plaintext archiveSha256. No archive key appears in stdout.

Upload 200/201 response must be bounded JSON `{checkpointId,ciphertextSha256,ciphertextBytes}`.
It means uploaded only. Task5 must fsync ciphertext, authenticate/decrypt it privately, decode
with `decodeCheckpointArchive`, verify every hash/mode/path/count, SQLite integrity/schema
and every Git object/ref, then retain the verified archive in durable private storage.
The length-framed archive begins `OPENRIG-CHECKPOINT-2\n`, then uint32-BE JSON-manifest length,
manifest bytes, then uint32-BE content length and bytes for each entry in manifest order.
Decoder returns inert bytes; it never extracts files or follows archive paths. File bytes
are bounded at 64 MiB each, source total 512 MiB, 100,000 paths; unsupported custody retains.
All Git metadata/objects and all declared runtime SQLite backup files are included. SQLite
backup uses the native backup API; WAL/SHM are not separate restoration artifacts. Final
rechecks compare all business bytes and complete SQLite schema/table row contents, so SQLite
connection housekeeping does not invalidate otherwise identical stopped custody. Logical
SQLite equality preserves full-width INTEGER values, REAL IEEE bits, TEXT/BLOB/NULL storage
classes, accessible rowids, all declared columns and database version/application/encoding
metadata. Unsupported rowid ambiguity fails closed.

`SignedCustodyReceipt` is `{body,signature}`. Body in serializer order:
`{version:1,binding,checkpointId,manifestDigest,ciphertextSha256,ciphertextBytes,verified:true,durableStoreId}`.
Sign UTF-8 `JSON.stringify(body)` using the prepared controller RSA private key, SHA256,
RSA-PSS padding and saltLength=32; base64 encode the signature. Receipt is issued only after
independent verified durable storage, never from HTTP upload success alone.

A crash leaving ciphertext without its descriptor fails closed and needs reconciliation;
retries never replace that ciphertext with newly randomized encryption. Expired upload
capabilities can be replaced only for the same checkpoint. No transfer/ack means retain the
stopped VM and its root-private archive indefinitely.

## Required real Linux proof before one-VM use

The focused tests cover real SQLite/Git/archive/crypto and lifecycle ordering with OS effects
stubbed. They do not prove the target image's systemd sandbox, source layout, account policy,
PID/cgroup/managed-IPC census, private TLS server, model execution, tool execution or reboot behavior.
Validate unit effective directives and foreground PID, deliberate escaped-UID retention,
missing/changed pin refusal, sealed boot/start/input/launch refusal, per-seat Pi model/tool
execution, shutdown history/full DB restore and verified final upload on Linux first.
Unsupported systemd directives or Pi paths are failed prerequisites; keep confinement intact.
Verified elastic startup suppresses the default kernel boot and unrelated Codex/Claude setup; plugin resources come only from the pinned vendored tree, and the Pi child disables extension discovery. Task5 must explicitly create the finite Pi rig/seats, confirm provider/tool execution and occupancy before scheduling; an empty DB is not model readiness.

Existing permanent hosts have no opt-in manifest/environment and retain their normal behavior.

## Conservative draining ingress boundary

Current V1 worker processing already in flight can finish during drain. New plain-text
input, automatic recovery and launch are denied while the native DB is draining. Although
session-transport has `auditPointer` and `committedOutboxIds`, the Pi runner final stdin
boundary cannot authenticate that lineage from its text alone. This implementation never
infers permission from an occupied seat or a caller-provided queue ID. Any owned work needing
another input/recovery remains retained and blocks deletion. Task5 must add a durable,
authenticated continuation envelope through the final runtime ingress, or a reviewed typed
pre-seal drain withdrawal/reconciliation that restores productive custody. A sealed worker
must never be unsealed. Local queue/human-return transactions already retain their actual custody.
This is an explicit fail-closed integration limitation, not completed continuation delivery.

## Build the complete worker bundle

Use an isolated Linux amd64 checkout at the reviewed commit. Run `npm ci` and
`npm run build:package` before invoking `node integrations/azure-elastic/build-worker-bundle.mjs <config.json>`.
The builder config contains `checkout`, `commit`, `lockSha256`, `piRoot`, `node`,
`nodeSha256`, `packageDirectory`, `packageRecipe`, and an absolute `output` tar path.
`piRoot` is the package root of Pi 1.0.4 with its complete installed dependency closure.
Its actual `bin.pi` is resolved from `package.json`. A nested npm package without its
hoisted dependencies is not a complete input. The builder preserves the built checkout,
the assembled CLI daemon/resources, native SQLite, and the compiled custody SQLite helper.
It emits SHA256, size, build provenance, and tree pins for `/opt/openrig`, `/opt/pi`, and
`/opt/node`. Required canonical `rig` and credential-loading `gh` launchers are regular files.
The worker uses `/opt/pi/dist/bundle/cli.js` for Pi 1.0.4.

The Ubuntu recipe type is exported as `UbuntuPackageRecipe` in
`packages/daemon/src/adapters/elastic-worker-enrollment.ts`. It binds Ubuntu 24.04/amd64,
explicit supported `basePackages` versions, every ordered `.deb` filename/package/version/
architecture/hash/size, and exact final git/gh/tmux/bash/prlimit/python3 hashes.
`signedMetadata` binds the pinned archive keyring and a finite set of signed InRelease files,
each with its own exact Packages index hash and release path. The builder verifies signatures,
index membership, and every package. The stager uses these already approved package bytes.

The fixed `stage-worker.py` receives a typed protected managed RunCommand request. It downloads
only `/api/elastic-artifacts/<sha256>` from the configured private IPv4 HTTPS origin, using the
separately supplied CA and optional certificate pin. TLS 1.3, IP SAN, digest, exact length,
finite deadlines, and redirect refusal are mandatory. It validates archive entries before copying.
It uses APT only with explicit verified local package filenames and isolated empty source/list
paths, after a simulation whose install/configure actions must be within the approved recipe.
There is no online package lookup. The pinned manifest is supplied separately; pins are never
regenerated on the VM. A partial package/copy/install state is retained for explicit reconciliation.
Python 3 with TLS 1.3 must already be available to run this trusted stager on the target image.

## Consume enrollment and native preparation

`ElasticWorkerEnrollment` exports `advanceEnrollment`, read-only `inspectEnrollment`,
`ensureElasticRegistration`, `observeManagedWorker`, and read-only `inspectNative`.
`bootstrap` resolves only for a currently ready native worker. The caller schedules bounded
`advanceEnrollment` calls for an existing operation; inspection does not submit commands.
`ElasticWorkerAttemptStore.custodyMaterial` returns the original private key and binding for
`CheckpointCustody.registerAttempt`. Clear the returned private Buffer after use.

`AzureWorkerCommandClient` requires an absolute Azure CLI executable, isolated Azure config
directory, and private command directory. It independently resolves the owned VMSS model,
child incarnation, protection, image, and unique attached private IPv4 before mutations.
Managed commands use a deterministic exact ARM CreateOrUpdate request with API `2024-11-01`,
`asyncExecution`, and named protected parameters. The CLI invocation is `az rest ... --body @<private-file>`.
The legacy CLI `vmss run-command create` wrapper parses protected parameters as name=value
and cannot safely consume a private JSON filename. Readback uses the same managed command
resource and its terminal instanceView result. A timeout or missing readback never authorizes
another PUT or shared-request overwrite.

The approved finite preparation plan is installed at `/etc/openrig-elastic/preparation.json`.
The authenticated `/api/elastic-worker/prepare` route accepts only its operation ID and digest.
The service clones full deterministic checkouts as the dedicated worker UID, then calls native
bootstrap plan/apply with the exact reviewed action keys. It verifies actual rig/node/session,
Pi runtime/model/cwd, current process identity, and separate tool-proof evidence. No project
scripts execute as root. The current template has the two canonical seats in the manifest example.

Readiness tasks belong to an existing registered coordinator owner. `owner.tasks` gives each
seat its immutable native qitem ID and nonce. The coordinator creates those tasks after worker
registration; `readinessTaskBody` returns the exact bounded shell command to put in each row.
The worker requires the completed claimed native row, matching origin/source/destination,
a paired successful Pi Bash call/result, and the exact nonce-bound proof artifact. An absent
owner or model/tool result keeps the worker nonschedulable. Task5b3 composes owner/task creation,
capacity scheduling, durable idle windows, and final custody/removal. The existing root process
probe is an idle/drain proof; busy capacity needs a separate trusted observation before activation.

All `/api/*` requests on an enrolled elastic worker require its worker bearer. Only the three
exact POST activity hook/usage paths defer to their existing activity-token checks. Ordinary hosts
retain their existing API behavior. Local CLI lookup reads the root-owned group-readable
`local-api.json` only for its exact bound endpoint; explicit or registered remote URLs receive
no implicit local token. Requests with this local credential refuse redirects.

Public repository mode remains read-only. Optional approved narrow material uses the separate
`github-credential-install` encrypted operation, expected previous revision, expiry, and exact
repository permissions. `advanceGithubCredential` accepts an explicit replacement source.
The gh launcher rereads the fixed private record on every invocation and scopes GH_TOKEN to
its gh child. Dedicated HOME Git configuration uses native `gh auth git-credential`.
No issuer, GitHub App signing key, OAuth home, or controller bearer is distributed.
