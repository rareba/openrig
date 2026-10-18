import { constants, closeSync, existsSync, linkSync, unlinkSync, readdirSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { checkpointFailure } from "./elastic-checkpoint-contract.js";
/** Dedicated operator-configured retained controller store; never an archived worker path. */
export class CheckpointPrivateStore {
    readonly root: string;
    constructor(root: string) {
        if (resolve(root) !== root || realpathSync(root) !== root)
            checkpointFailure();
        this.root = root;
        this.directory(root);
        for (let at = dirname(root);; at = dirname(at)) {
            const ancestor = lstatSync(at);
            const trustedSticky = ancestor.uid === 0 && (ancestor.mode & 0o1000) !== 0;
            if (!ancestor.isDirectory() || realpathSync(at) !== at || (ancestor.uid !== 0 && ancestor.uid !== process.getuid?.()) || ((ancestor.mode & 0o022) !== 0 && !trustedSticky))
                checkpointFailure();
            if (at === "/")
                break;
        }
    }
    private confined(path: string): void { if (resolve(path) !== path || !(path === this.root || path.startsWith(this.root + "/")))
        checkpointFailure(); }
    directory(path: string): void {
        this.confined(path);
        const s = lstatSync(path);
        if (!s.isDirectory() || s.isSymbolicLink() || s.uid !== process.getuid?.() || (s.mode & 0o777) !== 0o700 || realpathSync(path) !== path)
            checkpointFailure();
    }
    sync(path: string): void { this.confined(path); const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW); try {
        fsyncSync(fd);
    }
    finally {
        closeSync(fd);
    } }
    existing(name: string): string {
        if (!/^[a-f0-9]{64}$/.test(name)) checkpointFailure();
        this.directory(this.root); const path=join(this.root,name); this.directory(path); return path;
    }
    child(name: string): string {
        if (!/^[a-f0-9]{64}$/.test(name))
            checkpointFailure();
        this.directory(this.root);
        const path = join(this.root, name);
        try {
            mkdirSync(path, { mode: 0o700 });
            this.sync(this.root);
        }
        catch (e) {
            if (!existsSync(path))
                throw e;
        }
        this.directory(path);
        return path;
    }
    read(path: string, max = 128 * 1024): Buffer {
        this.directory(dirname(path));
        const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
        try {
            let a = fstatSync(fd);
            // Recover a crash between exclusive hardlink publication and removal of its known private temporary link.
            if (a.isFile() && a.nlink === 2 && a.uid === process.getuid?.() && (a.mode & 0o777) === 0o600) {
                for (const name of readdirSync(dirname(path)).filter(n => /^pending-[a-f0-9]{32}$/.test(n) || /^upload-[a-f0-9]{32}\.partial$/.test(n))) {
                    const temporary = join(dirname(path), name);
                    const candidate = lstatSync(temporary);
                    if (candidate.isFile() && candidate.dev === a.dev && candidate.ino === a.ino) {
                        unlinkSync(temporary);
                        this.sync(dirname(path));
                        break;
                    }
                }
                a = fstatSync(fd);
            }
            if (!a.isFile() || a.nlink !== 1 || a.uid !== process.getuid?.() || (a.mode & 0o777) !== 0o600 || a.size > max)
                checkpointFailure();
            const b = readFileSync(fd);
            const after = fstatSync(fd);
            if (b.length !== a.size || a.ino !== after.ino || a.size !== after.size || a.mtimeMs !== after.mtimeMs)
                checkpointFailure();
            return b;
        }
        finally {
            closeSync(fd);
        }
    }
    /** Publish complete fsynced bytes without replacement; recover the exact same complete file after a crash. */
    write(path: string, bytes: Buffer | string): void {
        this.directory(dirname(path));
        const temporary = join(dirname(path), `pending-${randomBytes(16).toString("hex")}`);
        const fd = openSync(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
        try {
            writeFileSync(fd, bytes);
            fsyncSync(fd);
        }
        finally {
            closeSync(fd);
        }
        try {
            linkSync(temporary, path);
        }
        finally {
            unlinkSync(temporary);
        }
        this.sync(dirname(path));
    }
    allocatedBytes(): number {
        let total = 0, count = 0;
        const visit = (path: string): void => { this.directory(path); for (const name of readdirSync(path)) {
            if (++count > 200000)
                checkpointFailure();
            const child = join(path, name), stat = lstatSync(child);
            if (stat.isDirectory())
                visit(child);
            else {
                if (!stat.isFile() || stat.isSymbolicLink() || stat.uid !== process.getuid?.() || (stat.mode & 0o777) !== 0o600)
                    checkpointFailure();
                total += stat.size;
                if (!Number.isSafeInteger(total))
                    checkpointFailure();
            }
        } };
        visit(this.root);
        return total;
    }
    json(path: string): unknown { return JSON.parse(this.read(path).toString("utf8")); }
}
