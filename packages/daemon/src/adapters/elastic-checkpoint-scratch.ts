import { existsSync, mkdirSync, rmSync, statfsSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { CheckpointPrivateStore } from "./elastic-checkpoint-private-store.js";
import { checkpointFailure as fail } from "./elastic-checkpoint-contract.js";
/** All verification allocations share the retained store's allocated-block quota. */
export class CheckpointScratch {
    private readonly blockBytes: number;
    constructor(private readonly store: CheckpointPrivateStore, readonly root: string, private readonly ceiling: number) {
        this.blockBytes = statfsSync(store.root).bsize;
        if (!Number.isSafeInteger(this.blockBytes) || this.blockBytes < 512 || this.blockBytes > 1024 * 1024)
            fail();
        this.directory(root);
    }
    private capacity(bytes: number): void {
        const charge = Math.ceil(bytes / this.blockBytes) * this.blockBytes + 4 * this.blockBytes;
        if (this.store.allocatedBytes() + charge > this.ceiling)
            fail();
    }
    directory(path: string): void {
        if (!(path === this.root || path.startsWith(this.root + "/")))
            fail();
        if (existsSync(path)) {
            this.store.directory(path);
            return;
        }
        const parent = dirname(path);
        if (parent !== dirname(this.root))
            this.directory(parent);
        this.capacity(this.blockBytes);
        mkdirSync(path, { mode: 0o700 });
        if (this.store.allocatedBytes() > this.ceiling)
            fail();
    }
    write(path: string, bytes: Buffer | string): void {
        if (!path.startsWith(this.root + "/"))
            fail();
        this.directory(dirname(path));
        const data = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
        this.capacity(data.length);
        writeFileSync(path, data, { flag: "wx", mode: 0o600 });
        if (this.store.allocatedBytes() > this.ceiling)
            fail();
    }
    remove(path: string): void { if (!(path === this.root || path.startsWith(this.root + "/")))
        fail(); rmSync(path, { recursive: true, force: true }); }
}
