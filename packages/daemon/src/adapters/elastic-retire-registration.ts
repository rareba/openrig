import { existsSync, openSync, closeSync, writeFileSync, fsyncSync, renameSync, unlinkSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { stringify } from 'yaml';
import { withHostRegistryLock } from '../domain/hosts/hosts-registry-lock.js';
import { loadHostRegistry, validateHostRegistry } from '../domain/hosts/hosts-registry-reader.js';
import { managedHostId } from './elastic-worker-enrollment.js';
import { enrollmentDigest, type ElasticWorkerAttempt, type ElasticWorkerAttemptStore } from './elastic-worker-attempts.js';
/** Called only after the original exact delete intent has authoritative ARM absence. */
export function retireElasticRegistration(store: ElasticWorkerAttemptStore, attempt: ElasticWorkerAttempt, registryPath: string): void {
    const a = store.read(attempt.id);
    if (!a.registration || a.registration.hostId !== managedHostId(a.workerId))
        throw Error('elastic_registration_identity');
    withHostRegistryLock(registryPath, unchanged => {
        const loaded = loadHostRegistry(registryPath);
        if (!loaded.ok)
            throw Error('elastic_registry_invalid');
        const host = loaded.registry.hosts.find(h => h.id === a.registration!.hostId);
        if (!host) {
            if (!store.readPrivateInput(a.id, 'registration-retirement-intent'))
                throw Error('elastic_registration_absence_unproven');
            return;
        }
        if (host.transport !== 'http' || host.hostId !== a.registration!.selfHostId || host.url !== a.registration!.baseUrl || host.bearer_file !== store.tokenPath(a.id) || host.bearer_env)
            throw Error('elastic_registration_changed');
        const result = validateHostRegistry({ hosts: loaded.registry.hosts.filter(h => h.id !== host.id) }, registryPath);
        if (!result.ok)
            throw Error('elastic_registry_invalid');
        const intent = { registration: a.registration, hostDigest: enrollmentDigest(host) };
        if (enrollmentDigest(store.savePrivateInput(a.id, 'registration-retirement-intent', intent)) !== enrollmentDigest(intent))
            throw Error('elastic_registration_changed');
        const temp = join(dirname(registryPath), `.elastic-retire-${randomUUID()}`);
        const fd = openSync(temp, 'wx', 0o600);
        try {
            writeFileSync(fd, stringify({ hosts: result.registry.hosts }));
            fsyncSync(fd);
        }
        finally {
            closeSync(fd);
        }
        try {
            unchanged();
            renameSync(temp, registryPath);
            const parent = openSync(dirname(registryPath), 'r');
            try {
                fsyncSync(parent);
            }
            finally {
                closeSync(parent);
            }
        }
        finally {
            if (existsSync(temp))
                unlinkSync(temp);
        }
        const final = loadHostRegistry(registryPath);
        if (!final.ok || enrollmentDigest(final.registry.hosts) !== enrollmentDigest(result.registry.hosts))
            throw Error('elastic_registration_retirement_unknown');
    });
    store.savePrivateInput(a.id, 'registration-retired', { registration: a.registration });
}
