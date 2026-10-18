import { z } from "zod";
import type { EncryptedCheckpoint, FinalBinding } from "./elastic-worker-checkpoint.js";
export const checkpointDigest = z.string().regex(/^[a-f0-9]{64}$/);
const label = z.string().min(1).max(1024).regex(/^[^\x00-\x1f]+$/);
export const finalBindingSchema = z.object({ workerId: label, vmId: label, manifestDigest: checkpointDigest, fleetEpoch: label, drainGeneration: z.number().int().nonnegative().safe(), removalOperationId: label }).strict();
const base64 = z.string().max(16384).regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/);
export const checkpointDescriptorSchema = z.object({
    version: z.literal(1), binding: finalBindingSchema, checkpointId: checkpointDigest, manifestDigest: checkpointDigest,
    ciphertextSha256: checkpointDigest, ciphertextBytes: z.number().int().positive().max(528 * 1024 * 1024),
    envelope: z.object({ algorithm: z.literal("RSA-OAEP-SHA256/AES-256-GCM"), wrappedKey: base64, iv: base64, tag: base64, aad: z.string().max(8192), controllerKeyDigest: checkpointDigest }).strict(),
}).strict();
export type CheckpointDescriptor = Omit<EncryptedCheckpoint, "ciphertextPath">;
export const aadSchema = z.object({ version: z.literal(1), binding: finalBindingSchema, checkpointId: checkpointDigest, manifestDigest: checkpointDigest, archiveSha256: checkpointDigest }).strict();
export function checkpointFailure(): never { throw Error("checkpoint_custody_rejected"); }
export function sameBinding(a: FinalBinding, b: FinalBinding): boolean {
    return a.workerId === b.workerId && a.vmId === b.vmId && a.manifestDigest === b.manifestDigest && a.fleetEpoch === b.fleetEpoch && a.drainGeneration === b.drainGeneration && a.removalOperationId === b.removalOperationId;
}
export function canonicalBase64(raw: string, length: number): Buffer {
    const bytes = Buffer.from(raw, "base64");
    if (bytes.length !== length || bytes.toString("base64") !== raw)
        checkpointFailure();
    return bytes;
}
