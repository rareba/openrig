import { generateKeyPairSync } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { sealEnrollment, openEnrollment } from '../src/domain/elastic-enrollment.js';
const keys = generateKeyPairSync('rsa', { modulusLength: 3072 });
const other = generateKeyPairSync('rsa', { modulusLength: 3072 });
const binding = { version: 1 as const, resourceId: '/subscriptions/a/resourceGroups/b/providers/Microsoft.Compute/virtualMachineScaleSets/c', vmId: '11111111-1111-4111-8111-111111111111', nonce: Buffer.alloc(32, 7).toString('base64'), manifestDigest: 'a'.repeat(64), operationId: 'operation-1', fleetEpoch: 'epoch-1' };
describe('sealed Azure enrollment', () => {
 it('roundtrips a bounded credential without plaintext output and fresh encryption each time', () => {
  const one = sealEnrollment(Buffer.from('worker-private-bearer'), binding, keys.publicKey);
  const two = sealEnrollment(Buffer.from('worker-private-bearer'), binding, keys.publicKey);
  expect(one).not.toContain('worker-private-bearer'); expect(one).not.toBe(two);
  expect(openEnrollment(one, binding, keys.privateKey).toString()).toBe('worker-private-bearer');
 });
 it.each(['resourceId', 'vmId', 'nonce', 'manifestDigest', 'operationId', 'fleetEpoch'] as const)('rejects wrong %s binding', key => {
  const envelope = sealEnrollment(Buffer.from('secret'), binding, keys.publicKey);
  expect(() => openEnrollment(envelope, { ...binding, [key]: key === 'manifestDigest' ? 'b'.repeat(64) : key === 'nonce' ? Buffer.alloc(32, 8).toString('base64') : 'wrong' }, keys.privateKey)).toThrow('invalid_enrollment');
 });
 it('rejects tamper, wrong key, truncation, noncanonical base64, excess fields and oversized input', () => {
  const envelope = sealEnrollment(Buffer.from('secret'), binding, keys.publicKey);
  expect(() => openEnrollment(envelope, binding, other.privateKey)).toThrow('invalid_enrollment');
  for (const value of [envelope.slice(0, -1), ' '.repeat(5000), JSON.stringify({...JSON.parse(envelope), extra: true}), JSON.stringify({...JSON.parse(envelope), ciphertext: '!!!!'}), JSON.stringify({...JSON.parse(envelope), tag: Buffer.alloc(16).toString('base64')})]) expect(() => openEnrollment(value, binding, keys.privateKey)).toThrow('invalid_enrollment');
  expect(() => sealEnrollment(Buffer.alloc(2048), binding, keys.publicKey)).toThrow('invalid_enrollment');
 });
 it('rejects weak and non-RSA wrapping keys', () => {
  const weak = generateKeyPairSync('rsa', {modulusLength: 1024});
  const ec = generateKeyPairSync('ec', {namedCurve: 'prime256v1'});
  expect(() => sealEnrollment(Buffer.from('secret'), binding, weak.publicKey)).toThrow('invalid_enrollment');
  expect(() => sealEnrollment(Buffer.from('secret'), binding, ec.publicKey)).toThrow('invalid_enrollment');
 });
 it('rejects truncated cryptographic fields, duplicate JSON and malformed binding', () => {
  const envelope=sealEnrollment(Buffer.from('secret'),binding,keys.publicKey);
  for(const field of ['wrappedKey','iv','tag','ciphertext']) {
   const parsed=JSON.parse(envelope);parsed[field]=Buffer.alloc(1).toString('base64');
   expect(()=>openEnrollment(JSON.stringify(parsed),binding,keys.privateKey)).toThrow('invalid_enrollment');
  }
  expect(()=>openEnrollment(envelope.replace('"binding":','"tag":"AAAA","binding":'),binding,keys.privateKey)).toThrow('invalid_enrollment');
  expect(()=>sealEnrollment(Buffer.from('secret'),{...binding,vmId:'-'.repeat(36)},keys.publicKey)).toThrow('invalid_enrollment');
  expect(()=>openEnrollment(envelope,{...binding,version:2} as never,keys.privateKey)).toThrow('invalid_enrollment');
 });

});
