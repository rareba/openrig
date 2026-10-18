import { createPrivateKey, X509Certificate, createPublicKey } from "node:crypto";
import { createServer, type Server } from "node:https";
import type { IncomingMessage, ServerResponse } from "node:http";
import { checkpointDescriptorSchema, checkpointFailure as fail } from "./elastic-checkpoint-contract.js";
import { CheckpointCustody } from "./elastic-checkpoint-custody.js";
import { privateIPv4 } from "./linux-privileged-worker-boundary.js";
const PREFIX = "/api/elastic-checkpoints/";
/** Upload-only handler, also usable with an ephemeral local TLS test fixture. */
export function checkpointReceiverHandler(custody: CheckpointCustody): (req: IncomingMessage, res: ServerResponse) => void {
    return (req, res) => {
        void (async () => {
            try {
                if (req.method !== "PUT" || !req.url || !new RegExp(`^${PREFIX}[a-f0-9]{64}$`).test(req.url) || req.rawHeaders.length > 24)
                    fail();
                const seen = new Set<string>();
                const allowed = new Set(["host", "connection", "authorization", "content-type", "content-length", "x-openrig-checkpoint"]);
                for (let i = 0; i < req.rawHeaders.length; i += 2) {
                    const name = req.rawHeaders[i]!.toLowerCase();
                    if (seen.has(name) || !allowed.has(name))
                        fail();
                    seen.add(name);
                }
                if (req.headers["content-type"] !== "application/octet-stream" || !/^Bearer [A-Za-z0-9_-]{32,256}$/.test(req.headers.authorization ?? "") || typeof req.headers["x-openrig-checkpoint"] !== "string" || req.headers["x-openrig-checkpoint"].length > 16384)
                    fail();
                const metadata = req.headers["x-openrig-checkpoint"];
                const bytes = Buffer.from(metadata, "base64");
                if (bytes.toString("base64") !== metadata)
                    fail();
                const raw = bytes.toString("utf8");
                const d = checkpointDescriptorSchema.parse(JSON.parse(raw));
                if (JSON.stringify(d) !== raw || req.headers["content-length"] !== String(d.ciphertextBytes))
                    fail();
                const result = await custody.receive({ checkpointId: req.url.slice(PREFIX.length), token: req.headers.authorization!.slice(7), descriptor: d, body: req });
                res.writeHead(201, { "content-type": "application/json", "connection": "close" });
                res.end(JSON.stringify({ checkpointId: result.checkpointId, ciphertextSha256: result.ciphertextSha256, ciphertextBytes: result.ciphertextBytes }));
            }
            catch {
                if (!res.headersSent)
                    res.writeHead(400, { "content-type": "application/json", "connection": "close" });
                res.end('{"error":"checkpoint_custody_rejected"}');
            }
        })();
    };
}
export interface PrivateCheckpointListenerOptions {
    bindIPv4: string;
    port: number;
    certificatePem: Buffer;
    tlsPrivateKeyPem: Buffer;
    certificateSha256: string;
}
/** Exact private bind and pinned server certificate; enrollment signing key is separately held by custody. */
export function validatePrivateCheckpointListener(options: PrivateCheckpointListenerOptions): void {
    try {
        if (!privateIPv4(options.bindIPv4) || !Number.isSafeInteger(options.port) || options.port < 1024 || options.port > 65535)
            fail();
        const certificate = new X509Certificate(options.certificatePem);
        const key = createPrivateKey(options.tlsPrivateKeyPem);
        if (!/^[a-f0-9]{64}$/.test(options.certificateSha256) || certificate.fingerprint256.replaceAll(":", "").toLowerCase() !== options.certificateSha256 || certificate.checkIP(options.bindIPv4) !== options.bindIPv4 || !certificate.checkPrivateKey(key) || Date.parse(certificate.validFrom) > Date.now() || Date.parse(certificate.validTo) <= Date.now())
            fail();
        if (!createPublicKey(key).export({ type: "spki", format: "der" }).length)
            fail();
    }
    catch {
        return fail();
    }
}
export async function listenPrivateCheckpoints(custody: CheckpointCustody, options: PrivateCheckpointListenerOptions): Promise<{
    close: () => Promise<void>;
}> {
    validatePrivateCheckpointListener(options);
    custody.assertSeparateTlsKey(createPublicKey(createPrivateKey(options.tlsPrivateKeyPem)).export({ type: "spki", format: "der" }));
    const server: Server = createServer({ cert: options.certificatePem, key: options.tlsPrivateKeyPem, minVersion: "TLSv1.3", maxVersion: "TLSv1.3", maxHeaderSize: 20000, headersTimeout: 5000, requestTimeout: 30000, keepAliveTimeout: 1 }, checkpointReceiverHandler(custody));
    server.setTimeout(30000, socket => socket.destroy());
    server.maxConnections = 4;
    server.maxHeadersCount = 12;
    server.on("clientError", (_error, socket) => socket.destroy());
    await new Promise<void>((resolve, reject) => { const failed = () => reject(Error("checkpoint_custody_rejected")); server.once("error", failed); server.listen({ host: options.bindIPv4, port: options.port, exclusive: true }, () => { server.off("error", failed); resolve(); }); });
    return { close: () => new Promise<void>((resolve, reject) => { server.close(error => error ? reject(Error("checkpoint_custody_rejected")) : resolve()); server.closeAllConnections(); }) };
}
