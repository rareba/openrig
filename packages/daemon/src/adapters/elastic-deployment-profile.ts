import { z } from 'zod';
import { MAX_RUNTIME_ARTIFACT_BYTES } from './elastic-runtime-artifact.js';
import { lstatSync, readFileSync, realpathSync, openSync, closeSync, fstatSync, constants } from 'node:fs';
import { createHash, X509Certificate } from 'node:crypto';
import { isAbsolute, dirname } from 'node:path';
import { CheckpointPrivateStore } from './elastic-checkpoint-private-store.js';
import { preparationPlanSchema } from './elastic-worker-prepare.js';
import { parseLinuxManifest } from './linux-privileged-worker-boundary.js';
import { validatePrivateCheckpointListener } from './elastic-checkpoint-receiver.js';
const path = z.string().max(4096).refine(p => isAbsolute(p) && !p.includes('\0'));
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const label = z.string().regex(/^[A-Za-z0-9_.:@/-]{1,256}$/);
const pin = z.object({ path, sha256: digest }).strict();
export const elasticDeploymentProfileSchema = z.object({
    version: z.literal(1), fleetEpoch: label, workerBuildCommit: z.string().regex(/^[a-f0-9]{40}$/),
    coordinatorManager: label, coordinatorHuman: label, originHuman: label,
    manifestPath: path, preparationPlanPath: path, packageRecipePath: path, registryPath: path,
    attemptDirectory: path, commandDirectory: path,
    azure: z.object({ executable: path, configDirectory: path }).strict(),
    stageScript: pin, helperScript: pin,
    artifact: z.object({ path, archiveSha256: digest, archiveBytes: z.number().int().positive().max(MAX_RUNTIME_ARTIFACT_BYTES) }).strict(),
    tls: z.object({ bindIPv4: z.string(), port: z.number().int(), certificatePath: path, keyPath: path, caPath: path, certificateSha256: digest }).strict(),
    custody: z.object({ directory: path, durableStoreId: label, maxBytes: z.number().int().positive().max(528 * 1024 ** 2), maxReservedBytes: z.number().int().positive(), maxStoredBytes: z.number().int().positive().max(1024 ** 4), retentionPolicyId: label, maxCheckpoints: z.number().int().min(1).max(1024) }).strict(),
}).strict();
export type ElasticDeploymentProfile = z.infer<typeof elasticDeploymentProfileSchema>;
export function readPrivateProfileFile(path: string, maxBytes = 1024 * 1024): Buffer {
    const s = lstatSync(path);
    if (realpathSync(path) !== path || !s.isFile() || s.nlink !== 1 || s.uid !== process.getuid?.() || (s.mode & 0o077) !== 0 || s.size > maxBytes)
        throw Error('elastic_private_file_rejected');
    for (let at = dirname(path);; at = dirname(at)) {
        const parent = lstatSync(at);
        const sticky = parent.uid === 0 && (parent.mode & 0o1000) !== 0;
        if (!parent.isDirectory() || realpathSync(at) !== at || (parent.uid !== 0 && parent.uid !== process.getuid?.()) || ((parent.mode & 0o022) !== 0 && !sticky))
            throw Error('elastic_private_parent_rejected');
        if (at === '/')
            break;
    }
    const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
        const actual = fstatSync(fd);
        if (actual.dev !== s.dev || actual.ino !== s.ino)
            throw Error('elastic_private_file_changed');
        const bytes = readFileSync(fd);
        if (bytes.length !== s.size || fstatSync(fd).mtimeMs !== s.mtimeMs)
            throw Error('elastic_private_file_changed');
        return bytes;
    }
    finally {
        closeSync(fd);
    }
}
export function loadElasticDeploymentProfile(profilePath: string) {
    const profile = elasticDeploymentProfileSchema.parse(JSON.parse(readPrivateProfileFile(profilePath).toString()));
    const manifestRaw = readPrivateProfileFile(profile.manifestPath, 65536).toString();
    const manifest = parseLinuxManifest(manifestRaw);
    const preparationPlan = preparationPlanSchema.parse(JSON.parse(readPrivateProfileFile(profile.preparationPlanPath, 65536).toString()));
    const certificatePem = readPrivateProfileFile(profile.tls.certificatePath, 65536);
    const tlsPrivateKeyPem = readPrivateProfileFile(profile.tls.keyPath, 16384);
    const caPem = readPrivateProfileFile(profile.tls.caPath, 65536).toString();
    const listener = { bindIPv4: profile.tls.bindIPv4, port: profile.tls.port, certificatePem, tlsPrivateKeyPem, certificateSha256: profile.tls.certificateSha256 };
    validatePrivateCheckpointListener(listener);
    if (preparationPlan.manifestDigest !== createHash('sha256').update(manifestRaw).digest('hex'))
        throw Error('elastic_manifest_changed');
    const ca = new X509Certificate(caPem), certificate = new X509Certificate(certificatePem);
    if (!ca.ca || !certificate.checkIssued(ca) || !certificate.verify(ca.publicKey) || Date.parse(ca.validFrom) > Date.now() || Date.parse(ca.validTo) <= Date.now())
        throw Error('elastic_tls_ca_rejected');
    const origin = `https://${profile.tls.bindIPv4}:${profile.tls.port}`;
    if (new URL(manifest.linux.upload.origin).origin !== origin || manifest.linux.upload.privateIPv4 !== profile.tls.bindIPv4 || profile.custody.maxBytes > manifest.linux.upload.maxBytes)
        throw Error('elastic_transfer_origin_conflict');
    if (preparationPlan.seats.map(s => s.sessionName).join(',') !== 'worker-a@elastic,worker-b@elastic' || manifest.linux.seats.join(',') !== 'worker-a@elastic,worker-b@elastic')
        throw Error('elastic_finite_seats_required');
    const directories = [profile.attemptDirectory, profile.commandDirectory, profile.custody.directory];
    for (const directory of directories) {
        new CheckpointPrivateStore(directory);
        if (directories.some(other => other !== directory && (directory.startsWith(other + '/') || other.startsWith(directory + '/'))) || manifest.ownedRoots.some(root => directory === root || directory.startsWith(root + '/')))
            throw Error('elastic_private_store_conflict');
    }
    if (new Set(directories).size !== directories.length)
        throw Error('elastic_private_store_conflict');
    for (const script of [profile.stageScript, profile.helperScript]) {
        const st = lstatSync(script.path);
        if (realpathSync(script.path) !== script.path || !st.isFile() || st.nlink !== 1 || st.mode & 0o022 || st.size > 128 * 1024 || (st.uid !== 0 && st.uid !== process.getuid?.()) || createHash('sha256').update(readFileSync(script.path)).digest('hex') !== script.sha256)
            throw Error('elastic_script_pin_rejected');
    }
    const packageRecipe = ubuntuRecipeSchema.parse(JSON.parse(readPrivateProfileFile(profile.packageRecipePath).toString()));
    return { profile, manifestRaw, manifest, preparationPlan, listener, caPem, packageRecipe,
        artifact: { version: 1 as const, url: `${origin}/api/elastic-artifacts/${profile.artifact.archiveSha256}`, privateIPv4: profile.tls.bindIPv4, archiveSha256: profile.artifact.archiveSha256, archiveBytes: profile.artifact.archiveBytes, caPem, caSha256: createHash('sha256').update(caPem).digest('hex'), certificateSha256: profile.tls.certificateSha256 } };
}
const packageFile = z.string().regex(/^[A-Za-z0-9_.+%~-]+$/);
const ubuntuRecipeSchema = z.object({ distribution: z.literal('ubuntu'), release: z.literal('24.04'), architecture: z.literal('amd64'),
    basePackages: z.record(z.string(), z.array(z.string()).min(1).max(256)), baseInventorySha256: digest.optional(),
    packages: z.array(z.object({ file: packageFile, package: z.string().min(1), version: z.string().min(1), architecture: z.enum(['amd64', 'all']), sha256: digest, bytes: z.number().int().positive() }).strict()).min(1).max(256),
    finalTools: z.array(pin).length(6),
    signedMetadata: z.object({ keyringFile: packageFile, keyringSha256: digest, releases: z.array(z.object({ file: packageFile, sha256: digest, indexes: z.array(z.object({ file: packageFile, sha256: digest, releasePath: z.string().regex(/^[A-Za-z0-9_./+-]{1,256}$/).refine(p => !p.split('/').includes('..')) }).strict()).min(1).max(32) }).strict()).min(1).max(8) }).strict(),
}).strict();
