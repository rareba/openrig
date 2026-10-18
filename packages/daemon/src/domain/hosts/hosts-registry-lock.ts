import { closeSync, constants, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, realpathSync, rmdirSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { nativeOwnerIdentity, nativeOwnerStopped } from '../../adapters/elastic-native-owner.js';
/** All native CLI/daemon writers share this exact contract. Direct external editors remain
 * whole-file writers; observed drift fails closed, never described as filesystem CAS. */
export function withHostRegistryLock<T>(path: string, write: (unchanged: () => void) => T): T {
    mkdirSync(dirname(path), { recursive: true });
    const canonical = resolve(realpathSync(dirname(path)), path.slice(path.lastIndexOf('/') + 1));
    const lock = `${canonical}.writer-lock`;
    const recovery = `${lock}.recovery`;
    const deadline = Date.now() + 2000;
    const wait = () => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
    let fd: number | undefined;
    while (fd === undefined) {
        try {
            if (existsSync(recovery))
                throw Error('host_registry_recovery_unknown');
            fd = openSync(lock, constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
        }
        catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'EEXIST')
                throw error;
            try {
                const st = lstatSync(lock);
                if (!st.isFile() || st.isSymbolicLink() || st.uid !== process.getuid?.() || (st.mode & 0o077) !== 0)
                    throw Error();
                const bytes = readFileSync(lock, 'utf8');
                const previous = JSON.parse(bytes) as {
                    owner: string;
                    identity: unknown;
                };
                if (!nativeOwnerStopped(previous.identity, nativeOwnerIdentity(lock)))
                    throw Error();
                mkdirSync(recovery, { mode: 0o700 });
                try {
                    const current = lstatSync(lock);
                    if (current.ino !== st.ino || current.dev !== st.dev || readFileSync(lock, 'utf8') !== bytes)
                        throw Error('host_registry_owner_changed');
                    unlinkSync(lock);
                    fd = openSync(lock, constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
                }
                finally {
                    rmdirSync(recovery);
                }
            }
            catch {
                if (Date.now() >= deadline)
                    throw Error('host_registry_writer_busy_or_unknown: retain writer-lock; native stopped-owner proof required');
                wait();
            }
        }
    }
    const opened = fstatSync(fd);
    const owner = randomUUID();
    try {
        // Unsupported native proof refuses automatic recovery, but ordinary first writes still work.
        let identity: unknown = null;
        try {
            identity = nativeOwnerIdentity(lock);
        }
        catch { /* Unknown locks remain retained after a crash. */ }
        writeFileSync(fd, JSON.stringify({ owner, identity }));
        fsyncSync(fd);
        const before = existsSync(canonical) ? readFileSync(canonical) : null;
        return write(() => { const now = existsSync(canonical) ? readFileSync(canonical) : null; if (before === null ? now !== null : now === null || !before.equals(now))
            throw Error('host_registry_external_drift'); });
    }
    finally {
        closeSync(fd);
        const current = lstatSync(lock);
        if (current.ino !== opened.ino || current.dev !== opened.dev || JSON.parse(readFileSync(lock, 'utf8')).owner !== owner)
            throw Error('host_registry_owner_changed');
        unlinkSync(lock);
    }
}
