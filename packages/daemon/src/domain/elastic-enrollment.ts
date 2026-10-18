import { constants, createCipheriv, createDecipheriv, privateDecrypt, publicEncrypt, randomBytes, type KeyObject } from 'node:crypto';

/** Persist this exact attempt binding and its private wrapping key before cloud execution. */
export interface EnrollmentBinding {
  version: 1;
  resourceId: string;
  vmId: string;
  nonce: string;
  manifestDigest: string;
  operationId: string;
  fleetEpoch: string;
}
const fields = ['version', 'resourceId', 'vmId', 'nonce', 'manifestDigest', 'operationId', 'fleetEpoch'];
function object(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function fail(): never { throw Error('invalid_enrollment'); }
function decode(value: unknown, min: number, max = min): Buffer {
  if (typeof value !== 'string' || value.length > Math.ceil(max / 3) * 4 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) fail();
  const bytes = Buffer.from(value, 'base64');
  if (bytes.length < min || bytes.length > max || bytes.toString('base64') !== value) fail();
  return bytes;
}
function aad(value: unknown): Buffer {
  if (!object(value) || Object.keys(value).length !== fields.length || !fields.every(key => Object.hasOwn(value, key)) || value.version !== 1) fail();
  for (const key of fields.slice(1)) if (typeof value[key] !== 'string' || value[key].length < 1 || value[key].length > 512 || /[\u0000-\u001f]/.test(value[key])) fail();
  if (!/^\/subscriptions\/[^/]+\/resourceGroups\/[^/]+\/providers\/Microsoft\.Compute\/virtualMachineScaleSets\/[^/]+$/.test(String(value.resourceId))
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(value.vmId)) || !/^[a-f0-9]{64}$/.test(String(value.manifestDigest))) fail();
  decode(value.nonce, 32);
  return Buffer.from(JSON.stringify(fields.map(key => value[key])));
}
function keySize(key: KeyObject, type: 'public' | 'private'): number {
  if (key.type !== type || key.asymmetricKeyType !== 'rsa') fail();
  const bits = key.asymmetricKeyDetails?.modulusLength;
  if (!bits || bits < 3072 || bits > 8192) fail();
  return Math.ceil(bits / 8);
}
/** Hybrid encryption only; origin authentication must come from the exact Azure operation. */
function seal(plaintext: Buffer, binding: EnrollmentBinding, publicKey: KeyObject, material: boolean, domain = 'openrig-worker-material-v1\0'): string {
  try {
    keySize(publicKey, 'public'); const associated = material ? Buffer.concat([Buffer.from(domain), aad(binding)]) : aad(binding);
    if (!Buffer.isBuffer(plaintext) || plaintext.length < 1 || plaintext.length > (material ? 64 * 1024 : 1024)) fail();
    const key = randomBytes(32); const iv = randomBytes(12);
    try {
      const cipher = createCipheriv('aes-256-gcm', key, iv); cipher.setAAD(associated);
      const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
      const wrappedKey = publicEncrypt({ key: publicKey, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' }, key);
      const result = JSON.stringify({ binding, wrappedKey: wrappedKey.toString('base64'), iv: iv.toString('base64'), ciphertext: ciphertext.toString('base64'), tag: cipher.getAuthTag().toString('base64') });
      if (Buffer.byteLength(result) > (material ? 96 * 1024 : 4096)) fail(); return result;
    } finally { key.fill(0); }
  } catch { return fail(); }
}
/** Accept exactly one complete bounded JSON envelope, bound to the persisted attempt. */
function open(envelope: string, expected: EnrollmentBinding, privateKey: KeyObject, material: boolean, domain = 'openrig-worker-material-v1\0'): Buffer {
  try {
    const size = keySize(privateKey, 'private'); const expectedAad = aad(expected); const associated = material ? Buffer.concat([Buffer.from(domain), expectedAad]) : expectedAad;
    if (typeof envelope !== 'string' || Buffer.byteLength(envelope) > (material ? 96 * 1024 : 4096)) fail();
    const parsed: unknown = JSON.parse(envelope);
    if (!object(parsed) || Object.keys(parsed).sort().join(',') !== 'binding,ciphertext,iv,tag,wrappedKey' || !aad(parsed.binding).equals(expectedAad)) fail();
    // Reject duplicate JSON fields and alternate encodings: the producer emits this canonical shape.
    if (JSON.stringify(parsed) !== envelope) fail();
    const key = privateDecrypt({ key: privateKey, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' }, decode(parsed.wrappedKey, size));
    try {
      if (key.length !== 32) fail();
      const decipher = createDecipheriv('aes-256-gcm', key, decode(parsed.iv, 12));
      decipher.setAAD(associated); decipher.setAuthTag(decode(parsed.tag, 16));
      return Buffer.concat([decipher.update(decode(parsed.ciphertext, 1, material ? 64 * 1024 : 1024)), decipher.final()]);
    } finally { key.fill(0); }
  } catch { return fail(); }
}

export function sealEnrollment(plaintext: Buffer, binding: EnrollmentBinding, publicKey: KeyObject): string { return seal(plaintext, binding, publicKey, false); }
export function openEnrollment(envelope: string, expected: EnrollmentBinding, privateKey: KeyObject): Buffer { return open(envelope, expected, privateKey, false); }
/** Separate AAD domain and 64 KiB plaintext limit for protected Run Command configuration input. */
export function sealWorkerMaterial(plaintext: Buffer, binding: EnrollmentBinding, publicKey: KeyObject): string { return seal(plaintext, binding, publicKey, true); }
export function openWorkerMaterial(envelope: string, expected: EnrollmentBinding, privateKey: KeyObject): Buffer { return open(envelope, expected, privateKey, true); }

/** Scoped transfer authorization is never interchangeable with runtime material or the fleet bearer. */
export function sealCheckpointCapability(plaintext: Buffer, binding: EnrollmentBinding, publicKey: KeyObject): string {
  if (plaintext.length > 8192) fail();
  return seal(plaintext, binding, publicKey, true, 'openrig-worker-checkpoint-capability-v1\0');
}
export function openCheckpointCapability(envelope: string, binding: EnrollmentBinding, privateKey: KeyObject): Buffer {
  if (Buffer.byteLength(envelope) > 16384) fail();
  const result = open(envelope, binding, privateKey, true, 'openrig-worker-checkpoint-capability-v1\0');
  if (result.length > 8192) { result.fill(0); fail(); }
  return result;
}

/** Repository credentials have a separate domain from immutable Pi runtime material. */
export function sealGithubMaterial(plaintext: Buffer, binding: EnrollmentBinding, publicKey: KeyObject): string {
  if (plaintext.length > 16384) fail();
  return seal(plaintext, binding, publicKey, true, 'openrig-worker-github-material-v1\0');
}
export function openGithubMaterial(envelope: string, binding: EnrollmentBinding, privateKey: KeyObject): Buffer {
  const bytes = open(envelope, binding, privateKey, true, 'openrig-worker-github-material-v1\0');
  if (bytes.length > 16384) { bytes.fill(0); fail(); }
  return bytes;
}
