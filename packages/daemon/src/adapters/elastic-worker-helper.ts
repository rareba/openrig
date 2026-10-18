import { GithubCredentialStore, githubMaterialSchema } from './elastic-worker-github.js';
import { createPrivateKey } from "node:crypto";
import { openCheckpointCapability, openGithubMaterial, type EnrollmentBinding } from "../domain/elastic-enrollment.js";
import Database from "better-sqlite3";
import { closeSync, openSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { z } from "zod";
import { AzureWorkerBootstrap } from "./azure-worker-bootstrap.js";
import { ElasticWorkerControl } from "../domain/elastic-worker-control.js";
import { assertElasticEffectAllowed } from "../domain/elastic-worker-guard.js";
import {
  LinuxPrivilegedWorkerBoundary,
  assertRootPath,
  parseLinuxManifest,
  pinDigest,
  readPrivateInput,
} from "./linux-privileged-worker-boundary.js";
import { noFollowRead, sha256 } from "./elastic-worker-checkpoint.js";
import { LinuxWorkerLifecycle } from "./linux-worker-lifecycle.js";
const text = z.string().min(1).max(1024);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const identity = z
  .object({ workerId: text, vmId: z.string().uuid(), manifestDigest: digest })
  .strict();
const enrollment = z
  .object({
    version: z.literal(1),
    vmId: z.string().uuid(),
    manifestDigest: digest,
    resourceId: text,
    nonce: text,
    operationId: text,
    fleetEpoch: text,
  })
  .strict();
const finalBinding = identity
  .extend({
    fleetEpoch: text,
    drainGeneration: z.number().int().positive(),
    removalOperationId: text,
  })
  .strict();
const capability = z
  .object({
    version: z.literal(1),
    binding: finalBinding,
    checkpointId: digest,
    token: z.string().regex(/^[A-Za-z0-9_-]{32,256}$/),
    expiresAt: z.number().int().positive(),
    maxBytes: z.number().int().positive(),
  })
  .strict();
const receipt = z
  .object({
    body: z
      .object({
        version: z.literal(1),
        binding: finalBinding,
        checkpointId: digest,
        manifestDigest: digest,
        ciphertextSha256: digest,
        ciphertextBytes: z.number().int().positive(),
        verified: z.literal(true),
        durableStoreId: text,
      })
      .strict(),
    signature: z.string().max(2048),
  })
  .strict();
const payloads = {
  prepare: z
    .object({
      binding: enrollment,
      controllerPublicKeyPem: z.string().max(8192),
    })
    .strict(),
  enroll: z
    .object({
      binding: enrollment,
      materialEnvelope: z.string().max(96 * 1024),
    })
    .strict(),
  "github-credential-install": z.object({ binding: enrollment, materialEnvelope: z.string().max(32 * 1024) }).strict(),
  "begin-drain": z.object({ identity, operationId: text }).strict(),
  quiesce: z.object({ binding: finalBinding }).strict(),
  "final-checkpoint-export": z
    .object({ binding: finalBinding, capabilityEnvelope: z.string().max(16384).optional() })
    .strict(),
  "final-checkpoint-upload": z.object({ binding: finalBinding, capabilityEnvelope: z.string().min(1).max(16384) }).strict(),
  "final-checkpoint-ack": z.object({ binding: finalBinding, receipt }).strict(),
  "seal-status": z.object({ binding: finalBinding }).strict(),
};
export function parseHelperPayload(operation: string, value: unknown): unknown {
  if (!Object.hasOwn(payloads, operation))
    throw Error("worker_bootstrap_rejected");
  return payloads[operation as keyof typeof payloads].parse(value);
}
export async function runWorkerHelper(argv: string[]): Promise<unknown> {
  if (argv.length !== 1) throw Error();
  const requested = argv[0]!;
  const operation = requested === "checkpoint-export" ? "final-checkpoint-export" : requested === "checkpoint-ack" ? "final-checkpoint-ack" : requested;
  const manifestPath = "/etc/openrig-elastic/manifest.json";
  assertRootPath(manifestPath);
  assertRootPath("/etc/openrig-elastic/manifest.sha256");
  const raw = noFollowRead(manifestPath, 65536);
  const digest = sha256(raw);
  if (
    noFollowRead("/etc/openrig-elastic/manifest.sha256", 128)
      .toString()
      .trim() !== digest
  )
    throw Error();
  const manifest = parseLinuxManifest(raw.toString());
  if (operation === "start-condition") {
    for (const pin of manifest.pins) if (pinDigest(pin.path, pin.kind) !== pin.sha256) throw Error();
    process.env.OPENRIG_ELASTIC_GUARD = manifest.linux.guard;
    process.env.OPENRIG_DB = manifest.linux.database;
    assertElasticEffectAllowed(undefined, { allowDraining: true });
    return { allowed: true };
  }
  const os = new LinuxPrivilegedWorkerBoundary(manifest, manifestPath, digest);
  os.assertTrustedEnvironment();
  const input = readPrivateInput(
    join(manifest.controlDirectory, "request.json"),
  );
  parseHelperPayload(operation, input);
  const db = new Database(manifest.linux.database, { fileMustExist: true });
  db.pragma("trusted_schema=OFF");
  const control = new ElasticWorkerControl(db);
  const bootstrap = new AzureWorkerBootstrap(manifest, digest, control, os);
  const lifecycle = new LinuxWorkerLifecycle(os, control);
  let lock: string | undefined;
  try {
    if (operation === "prepare") {
      const p = payloads.prepare.parse(input);
      const instance = os.instance();
      if (p.binding.vmId !== instance.vmId || p.binding.fleetEpoch !== instance.fleetEpoch || p.binding.operationId !== instance.operationId) throw Error();
      return bootstrap.prepare(p.binding, p.controllerPublicKeyPem);
    }
    if (operation === "enroll") {
      const p = payloads.enroll.parse(input);
      const instance = os.instance();
      if (
        p.binding.vmId !== instance.vmId ||
        p.binding.fleetEpoch !== instance.fleetEpoch ||
        p.binding.operationId !== instance.operationId
      )
        throw Error();
      return bootstrap.enroll(p.binding, p.materialEnvelope);
    }
    if (operation === "begin-drain") {
      const p = payloads["begin-drain"].parse(input);
      return bootstrap.beginDrain(p.identity, p.operationId);
    }
    const candidate = join(manifest.controlDirectory, "operation.lock");
    const fd = openSync(candidate, "wx", 0o600);
    closeSync(fd);
    lock = candidate;
    if (operation === "github-credential-install") {
      const p = payloads["github-credential-install"].parse(input);
      const prepared = JSON.parse(noFollowRead(join(manifest.controlDirectory, 'enrollment.json'), 16384).toString()) as { binding: EnrollmentBinding; privateKeyPem: string };
      const instance = os.instance(); const state = control.read();
      if (JSON.stringify(p.binding) !== JSON.stringify(prepared.binding) || instance.vmId !== p.binding.vmId || instance.fleetEpoch !== p.binding.fleetEpoch || instance.operationId !== p.binding.operationId || !state || state.phase !== 'ready' || state.vmId !== p.binding.vmId) throw Error();
      const plaintext = openGithubMaterial(p.materialEnvelope, prepared.binding, createPrivateKey(prepared.privateKeyPem));
      try { return new GithubCredentialStore('/etc/openrig-elastic', 0, manifest.gid).install(githubMaterialSchema.parse(JSON.parse(plaintext.toString()))); }
      finally { plaintext.fill(0); }
    }
    switch (operation) {
      case "quiesce":
        return lifecycle.quiesce(payloads.quiesce.parse(input).binding);
      case "final-checkpoint-export":
      case "final-checkpoint-upload": {
        const p = payloads[operation].parse(input);
        let cap;
        if (p.capabilityEnvelope) {
          const prepared = JSON.parse(noFollowRead(join(manifest.controlDirectory, 'enrollment.json'), 16384).toString()) as { binding: EnrollmentBinding; privateKeyPem: string };
          const plaintext = openCheckpointCapability(p.capabilityEnvelope, prepared.binding, createPrivateKey(prepared.privateKeyPem));
          try { cap = capability.parse(JSON.parse(plaintext.toString())); } finally { plaintext.fill(0); }
        }
        return await lifecycle.export(p.binding, cap);
      }
      case "final-checkpoint-ack": {
        const p = payloads["final-checkpoint-ack"].parse(input);
        return lifecycle.acknowledge(p.binding, p.receipt);
      }
      case "seal-status":
        return lifecycle.status(payloads["seal-status"].parse(input).binding);
      default:
        throw Error();
    }
  } finally {
    db.close();
    if (lock) unlinkSync(lock);
  }
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  try {
    const result = await runWorkerHelper(process.argv.slice(2));
    const output = typeof result === "string" ? result : JSON.stringify(result);
    if (Buffer.byteLength(output) + 1 > 4096) throw Error();
    process.stdout.write(output + "\n");
  } catch {
    process.stderr.write("worker_bootstrap_rejected\n");
    process.exitCode = 1;
  }
}
