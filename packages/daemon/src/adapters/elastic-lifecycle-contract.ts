import { z } from 'zod';
import { finalBindingSchema, checkpointDigest } from './elastic-checkpoint-contract.js';

const time = z.number().int().nonnegative().safe();
export const removalSealSchema = z.object({
  workerId: z.string().min(1), vmId: z.string().uuid(), operationId: z.string().min(1), observedAt: time,
  drainConfirmedAt: time, drainGeneration: z.string().regex(/^[1-9]\d*$/), beginDrainConfirmed: z.literal(true),
  fenced: z.literal(true), active: z.literal(0), claims: z.literal(0), processes: z.literal(0),
  pendingTransfers: z.literal(false), pendingActivationIntents: z.literal(false),
  coordinatorCheckpointAcknowledged: z.literal(true), clean: z.literal(true), pendingHumanReturns: z.literal(false),
}).strict();
export const finalStatusRequestSchema = z.object({ binding: finalBindingSchema,
  round: z.number().int().positive().safe(), nonce: z.string().regex(/^[a-f0-9]{64}$/),
}).strict();
export const finalStatusReceiptSchema = finalStatusRequestSchema.extend({
  seal: removalSealSchema, checkpointReceiptDigest: checkpointDigest,
}).strict();
export const drainReceiptSchema = z.object({ version: z.literal(1), workerId: z.string(), vmId: z.string().uuid(),
  manifestDigest: checkpointDigest, phase: z.literal('draining'), generation: z.number().int().positive(),
  drainOperationId: z.string(), drainStartedAt: time,
}).strict();
export const exportReceiptSchema = z.object({ checkpointId: checkpointDigest, manifestDigest: checkpointDigest,
  ciphertextSha256: checkpointDigest, ciphertextBytes: z.number().int().positive().max(528 * 1024 ** 2), uploaded: z.boolean(),
}).strict();
