import { withdrawalRequestSchema } from '../domain/elastic-continuation.js';
import { readdirSync } from 'node:fs';
import { join } from 'node:path';
import { z } from 'zod';
import { finalBindingSchema } from './elastic-checkpoint-contract.js';
import { drainReceiptSchema, exportReceiptSchema, finalStatusReceiptSchema } from './elastic-lifecycle-contract.js';
import { managedWorkerOperations } from './elastic-managed-operation.js';
import { enrollmentDigest, type ElasticWorkerAttemptStore } from './elastic-worker-attempts.js';
import type { ElasticNativeOwner } from './elastic-native-owner.js';
const digest = z.string().regex(/^[a-f0-9]{64}$/);
export const lifecycleCommandSchema = z.object({ name: z.string(), operation: z.enum(managedWorkerOperations), requestDigest: digest,
  state: z.enum(['intent', 'submitted', 'unknown', 'succeeded', 'failed']), inputName: z.string(), outputDigest: digest.optional(), result: z.unknown().optional(),
}).strict();
const roundSchema = z.object({ round: z.number().int().positive(), nonce: digest, ownerDigest: digest,
  revision: z.number().int().nonnegative(), command: lifecycleCommandSchema,
  receipt: finalStatusReceiptSchema.nullable(), disposition: z.enum(['pending', 'completed', 'stale']),
}).strict();
const withdrawalSchema=z.object({request:withdrawalRequestSchema,receipt:z.object({withdrawalId:z.string().uuid(),generation:z.number().int().positive(),actionId:z.string().uuid()}).strict().nullable(),finished:z.boolean()}).strict();
const stateSchema = z.object({ version: z.literal(1), sequence: z.number().int().nonnegative(), attemptId: digest,
  withdrawal: withdrawalSchema.nullable().optional(),
  previous: digest.nullable(), beginOperationId: z.string(), drain: drainReceiptSchema.nullable(),
  binding: finalBindingSchema.nullable(), removalOperationId: z.string().nullable(), operationRevision: z.number().int().nonnegative().nullable(),
  configurationDigest: digest.nullable(), commands: z.array(lifecycleCommandSchema),
  exported: exportReceiptSchema.nullable(), checkpointReceiptDigest: digest.nullable(), rounds: z.array(roundSchema),
  deleteIntent: z.object({ operationId: z.string(), revision: z.number().int(), resourceId: z.string(), instanceId: z.string(), vmId: z.string(),
    round: z.number().int(), requestDigest: digest, receiptDigest: digest, ownerDigest: digest,
  }).strict().nullable(), deleteConfirmed: z.boolean(), retired: z.boolean(),
}).strict();
export type LifecycleState = z.infer<typeof stateSchema>;
export type LifecycleCommand = z.infer<typeof lifecycleCommandSchema>;
/** Each revision is immutable and chained. Enrollment records and private keys are never rewritten. */
export class ElasticLifecycleJournal {
  constructor(private readonly store: ElasticWorkerAttemptStore, private readonly owner: ElasticNativeOwner) {}
  read(attemptId: string): LifecycleState | null {
    const names = readdirSync(join(this.store.root, attemptId)).filter(n => /^input-lifecycle-\d{10}\.json$/.test(n)).sort();
    let previous: LifecycleState | null = null;
    for (const file of names) {
      const next = stateSchema.parse(this.store.readPrivateInput(attemptId, file.slice(6, -5)));
      if (next.attemptId !== attemptId || next.sequence !== (previous ? previous.sequence + 1 : 0)
        || next.previous !== (previous ? enrollmentDigest(previous) : null)) throw Error('elastic_lifecycle_history_changed');
      previous = next;
    }
    return previous;
  }
  begin(attemptId: string, operationId: string): LifecycleState {
    const current = this.read(attemptId);
    if (current) {
      if(current.beginOperationId===operationId)return current;
      const reused=readdirSync(join(this.store.root,attemptId)).filter(n=>/^input-lifecycle-\d{10}\.json$/.test(n))
        .some(n=>stateSchema.parse(this.store.readPrivateInput(attemptId,n.slice(6,-5))).beginOperationId===operationId);
      if(reused)throw Error('elastic_drain_generation_conflict');
      if(!current.withdrawal?.finished || current.deleteIntent || current.commands.some(c=>c.operation==='quiesce'))throw Error('elastic_drain_generation_conflict');
      return this.write({version:1,sequence:current.sequence+1,attemptId,previous:enrollmentDigest(current),beginOperationId:operationId,withdrawal:null,
        drain:null,binding:null,removalOperationId:null,operationRevision:null,configurationDigest:null,commands:[],exported:null,checkpointReceiptDigest:null,rounds:[],deleteIntent:null,deleteConfirmed:false,retired:false});
    }
    return this.write({ version: 1, sequence: 0, attemptId, previous: null, withdrawal:null, beginOperationId: operationId, drain: null,
      binding: null, removalOperationId: null, operationRevision: null, configurationDigest: null, commands: [], exported: null,
      checkpointReceiptDigest: null, rounds: [], deleteIntent: null, deleteConfirmed: false, retired: false });
  }
  advance(current: LifecycleState, patch: Partial<Omit<LifecycleState, 'version' | 'sequence' | 'attemptId' | 'previous' | 'beginOperationId'>>): LifecycleState {
    if (enrollmentDigest(this.read(current.attemptId)) !== enrollmentDigest(current)) throw Error('elastic_lifecycle_revision_changed');
    return this.write({ ...current, ...patch, sequence: current.sequence + 1, previous: enrollmentDigest(current) });
  }
  private write(value: LifecycleState): LifecycleState {
    this.owner.assertHeld('fleet');
    const parsed = stateSchema.parse(value);
    const saved = this.store.savePrivateInput(value.attemptId, `lifecycle-${String(value.sequence).padStart(10, '0')}`, parsed);
    if (enrollmentDigest(saved) !== enrollmentDigest(parsed)) throw Error('elastic_lifecycle_revision_changed');
    return stateSchema.parse(saved);
  }
}
