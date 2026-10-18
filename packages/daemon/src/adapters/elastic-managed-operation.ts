/** Fixed root entry points; durable attempt records use the same finite vocabulary. */
export const managedWorkerOperations = [
    'stage', 'prepare', 'enroll', 'github-credential-install', 'begin-drain', 'quiesce',
    'final-checkpoint-export', 'final-checkpoint-upload', 'final-checkpoint-ack', 'seal-status',
] as const;
export type ManagedWorkerOperation = typeof managedWorkerOperations[number];
