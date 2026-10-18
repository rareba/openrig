import { createPublicKey, verify, constants } from "node:crypto";
import { request } from "node:https";
import { createReadStream } from "node:fs";
import type { LinuxWorkerManifest } from "./linux-privileged-worker-boundary.js";
import type {
  EncryptedCheckpoint,
  FinalBinding,
} from "./elastic-worker-checkpoint.js";
import { noFollowRead, sha256 } from "./elastic-worker-checkpoint.js";
import { workerDigest } from "../domain/elastic-worker-control.js";
export interface CheckpointCapability {
  version: 1;
  binding: FinalBinding;
  checkpointId: string;
  token: string;
  expiresAt: number;
  maxBytes: number;
}
export interface CustodyReceiptBody {
  version: 1;
  binding: FinalBinding;
  checkpointId: string;
  manifestDigest: string;
  ciphertextSha256: string;
  ciphertextBytes: number;
  verified: true;
  durableStoreId: string;
}
export interface SignedCustodyReceipt {
  body: CustodyReceiptBody;
  signature: string;
}
function fail(): never {
  throw Error("worker_checkpoint_transfer_rejected");
}
export function verifyCustodyReceipt(
  receipt: SignedCustodyReceipt,
  cp: EncryptedCheckpoint,
  keyPem: string,
): void {
  const body = receipt.body;
  if (
    !body ||
    Object.keys(body).sort().join(",") !==
      "binding,checkpointId,ciphertextBytes,ciphertextSha256,durableStoreId,manifestDigest,verified,version" ||
    body.version !== 1 ||
    body.verified !== true ||
    !body.durableStoreId ||
    body.durableStoreId.length > 512 ||
    body.checkpointId !== cp.checkpointId ||
    body.manifestDigest !== cp.manifestDigest ||
    body.ciphertextSha256 !== cp.ciphertextSha256 ||
    body.ciphertextBytes !== cp.ciphertextBytes ||
    workerDigest(body.binding) !== workerDigest(cp.binding) ||
    typeof receipt.signature !== "string" ||
    receipt.signature.length > 2048
  )
    fail();
  const key = createPublicKey(keyPem);
  if (
    key.asymmetricKeyType !== "rsa" ||
    (key.asymmetricKeyDetails?.modulusLength ?? 0) < 3072 ||
    !verify(
      "sha256",
      Buffer.from(JSON.stringify(body)),
      { key, padding: constants.RSA_PKCS1_PSS_PADDING, saltLength: 32 },
      Buffer.from(receipt.signature, "base64"),
    )
  )
    fail();
}
/** Fixed manifest TLS origin with pinned private connect address; no redirects or payload URL. */
export async function uploadCheckpoint(
  manifest: LinuxWorkerManifest,
  cp: EncryptedCheckpoint,
  cap: CheckpointCapability,
): Promise<{
  uploaded: true;
  checkpointId: string;
  ciphertextSha256: string;
  ciphertextBytes: number;
}> {
  const policy = manifest.linux.upload;
  if (
    cap.version !== 1 ||
    workerDigest(cap.binding) !== workerDigest(cp.binding) ||
    cap.checkpointId !== cp.checkpointId ||
    !Number.isSafeInteger(cap.expiresAt) ||
    cap.expiresAt <= Date.now() ||
    cap.expiresAt > Date.now() + 60 * 60 * 1000 ||
    !Number.isSafeInteger(cap.maxBytes) ||
    cap.maxBytes > policy.maxBytes ||
    cap.maxBytes < cp.ciphertextBytes ||
    !/^[A-Za-z0-9_-]{32,256}$/.test(cap.token) ||
    !/^[a-f0-9]{64}$/.test(cp.checkpointId)
  )
    fail();
  if (
    sha256(noFollowRead(cp.ciphertextPath, 600 * 1024 * 1024)) !==
    cp.ciphertextSha256
  )
    fail();
  const metadata = Buffer.from(
    JSON.stringify({
      version: 1,
      binding: cp.binding,
      checkpointId: cp.checkpointId,
      manifestDigest: cp.manifestDigest,
      ciphertextSha256: cp.ciphertextSha256,
      ciphertextBytes: cp.ciphertextBytes,
      envelope: cp.envelope,
    }),
  ).toString("base64");
  if (metadata.length > 16384) fail();
  const url = new URL(`${policy.routePrefix}${cp.checkpointId}`, policy.origin);
  await new Promise<void>((resolve, reject) => {
    const req = request(
      url,
      {
        method: "PUT",
        ca: noFollowRead(policy.caFile, 65536),
        rejectUnauthorized: true,
        minVersion: "TLSv1.3",
        lookup: (_host, _opts, callback) =>
          callback(null, policy.privateIPv4, 4),
        headers: {
          authorization: `Bearer ${cap.token}`,
          "content-type": "application/octet-stream",
          "content-length": String(cp.ciphertextBytes),
          "x-openrig-checkpoint": metadata,
        },
        timeout: 30000,
      },
      (res) => {
        let size = 0;
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => {
          size += chunk.length;
          if (size > 4096) {
            res.destroy();
            reject(Error("worker_checkpoint_transfer_rejected"));
          } else chunks.push(chunk);
        });
        res.on("end", () => {
          try {
            if (res.statusCode !== 200 && res.statusCode !== 201) fail();
            const body = JSON.parse(Buffer.concat(chunks).toString());
            if (
              body.checkpointId !== cp.checkpointId ||
              body.ciphertextSha256 !== cp.ciphertextSha256 ||
              body.ciphertextBytes !== cp.ciphertextBytes
            )
              fail();
            resolve();
          } catch {
            reject(Error("worker_checkpoint_transfer_rejected"));
          }
        });
        res.on("error", () =>
          reject(Error("worker_checkpoint_transfer_rejected")),
        );
      },
    );
    req.on("timeout", () => req.destroy());
    req.on("error", () => reject(Error("worker_checkpoint_transfer_rejected")));
    const stream = createReadStream(cp.ciphertextPath);
    stream.on("error", () => {
      req.destroy();
      reject(Error("worker_checkpoint_transfer_rejected"));
    });
    req.on("close", () => stream.destroy());
    stream.pipe(req);
  });
  return {
    uploaded: true,
    checkpointId: cp.checkpointId,
    ciphertextSha256: cp.ciphertextSha256,
    ciphertextBytes: cp.ciphertextBytes,
  };
}
