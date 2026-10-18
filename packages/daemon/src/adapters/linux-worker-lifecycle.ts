import { finalStatusRequestSchema, finalStatusReceiptSchema } from './elastic-lifecycle-contract.js';
import { inspectManagedTmuxIpc, retireStoppedTmuxIpc, type ManagedTmuxIpc } from "./managed-tmux-ipc.js";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import {
  ElasticWorkerControl,
  workerDigest,
} from "../domain/elastic-worker-control.js";
import { azureWorkerId, type AzureRemovalSeal } from "./azure-elastic-fleet.js";
import {
  captureFinalCheckpoint,
  encryptCheckpoint,
  durableWrite,
  noFollowRead,
  stoppedSourceDigest,
  type FinalBinding,
  type FinalCheckpoint,
  type EncryptedCheckpoint,
} from "./elastic-worker-checkpoint.js";
import {
  LinuxPrivilegedWorkerBoundary,
  linuxCommand,
} from "./linux-privileged-worker-boundary.js";
import type { WorkerFilesystemProof } from "./elastic-worker-probe.js";
import {
  verifyCustodyReceipt,
  uploadCheckpoint,
  type CheckpointCapability,
  type SignedCustodyReceipt,
} from "./elastic-checkpoint-upload.js";
function fail(): never {
  throw Error("worker_bootstrap_rejected");
}
export class LinuxWorkerLifecycle {
  constructor(
    readonly os: LinuxPrivilegedWorkerBoundary,
    readonly control: ElasticWorkerControl,
  ) {}
  private path(name: string) {
    return join(this.os.manifest.controlDirectory, name);
  }
  private read<T>(name: string): T {
    return JSON.parse(
      noFollowRead(this.path(name), 16 * 1024 * 1024).toString(),
    ) as T;
  }
  private save(name: string, value: unknown) {
    durableWrite(this.path(name), JSON.stringify(value));
  }
  private state(binding: FinalBinding) {
    const m = this.os.manifest;
    const instance = this.os.instance();
    const state = this.control.read();
    if (
      !state ||
      state.workerId !== binding.workerId ||
      state.vmId !== binding.vmId ||
      state.manifestDigest !== binding.manifestDigest ||
      state.generation !== binding.drainGeneration ||
      state.drainOperationId !== binding.removalOperationId ||
      binding.workerId !== azureWorkerId(m.resourceId, instance.vmId) ||
      binding.fleetEpoch !== instance.fleetEpoch ||
      binding.manifestDigest !== this.os.manifestDigest ||
      !binding.removalOperationId ||
      binding.removalOperationId.length > 512
    )
      fail();
    return state;
  }
  private stopped(binding: FinalBinding) {
    if (
      this.state(binding).phase !== "sealed" ||
      JSON.parse(noFollowRead(this.os.manifest.linux.guard, 4096).toString())
        .phase !== "sealed"
    )
      fail();
    for (let i = 0; i < 2; i++) {
      const p = this.os.processes();
      if (
        !p.complete ||
        !p.idle ||
        p.processes !== 0 ||
        Date.now() - p.observedAt > 5000
      )
        fail();
    }
    if (!this.control.ledger().clean) fail();
  }
  private filesystem(): WorkerFilesystemProof {
    const m = this.os.manifest;
    const raw = execFileSync(m.linux.node, [m.linux.probe], {
      uid: m.uid,
      gid: m.gid,
      env: {
        PATH: "/usr/bin:/bin",
        HOME: m.linux.home,
        TMUX_TMPDIR: m.linux.temp,
        OPENRIG_ELASTIC_MANIFEST: this.os.manifestPath,
        OPENRIG_ELASTIC_MANIFEST_DIGEST: this.os.manifestDigest,
      },
      stdio: ["ignore", "pipe", "pipe"],
      encoding: "utf8",
      timeout: 60000,
      maxBuffer: 16 * 1024 * 1024,
    });
    const proof = JSON.parse(raw) as WorkerFilesystemProof;
    if (!proof.complete || proof.blockers.length) fail();
    return proof;
  }
  quiesce(binding: FinalBinding): { quiesced: true; binding: FinalBinding } {
    const state = this.state(binding);
    if (state.phase === "ready") fail();
    const existing = existsSync(this.path("quiesced.json"));
    if (
      existing &&
      workerDigest(
        this.read<{ binding: FinalBinding }>("quiesced.json").binding,
      ) !== workerDigest(binding)
    )
      fail();
    if (state.phase !== "sealed") {
      const p = this.os.processes();
      if (
        !this.control.ledger().clean ||
        !p.complete ||
        !p.idle ||
        Date.now() - p.observedAt > 5000
      )
        fail();
      this.filesystem();
      const managedIpc = inspectManagedTmuxIpc(this.os.manifest);
      this.save("quiesced.json", { binding, phase: "sealing", managedIpc });
      this.control.sealAdmission(state, binding.drainGeneration);
    } else if (!existing) fail();
    this.os.installSealMarker();
    this.os.stopUnit();
    this.stopped(binding);
    const record = this.read<{ managedIpc: ManagedTmuxIpc[]; ipcAudit?: unknown }>("quiesced.json");
    const ipcAudit = retireStoppedTmuxIpc(this.os.manifest, record.managedIpc);
    this.stopped(binding);
    this.save("quiesced.json", { binding, phase: "stopped", managedIpc: record.managedIpc, ipcAudit: record.ipcAudit ?? ipcAudit });
    return { quiesced: true, binding };
  }
  async export(
    binding: FinalBinding,
    cap?: CheckpointCapability,
  ): Promise<{
    checkpointId: string;
    manifestDigest: string;
    ciphertextSha256: string;
    ciphertextBytes: number;
    uploaded: boolean;
  }> {
    this.stopped(binding);
    if (
      workerDigest(
        this.read<{ binding: FinalBinding }>("quiesced.json").binding,
      ) !== workerDigest(binding)
    )
      fail();
    let cp: FinalCheckpoint;
    let encrypted: EncryptedCheckpoint;
    const prepared = this.read<{ controllerPublicKeyPem: string }>(
      "enrollment.json",
    );
    if (existsSync(this.path("final-checkpoint.json"))) {
      cp = this.read("final-checkpoint.json");
      if (
        workerDigest(cp.binding) !== workerDigest(binding) ||
        stoppedSourceDigest(
          this.os.manifest.ownedRoots,
          this.os.manifest.linux.databases,
        ) !== cp.sourceDigest
      )
        fail();
      encrypted = encryptCheckpoint(cp, prepared.controllerPublicKeyPem);
    } else {
      const fs = this.filesystem();
      const journal = Buffer.from(
        linuxCommand("/usr/bin/journalctl", [
          "--unit",
          this.os.manifest.unit,
          "--output=json",
          "--no-pager",
          "--all",
        ]),
      );
      cp = await captureFinalCheckpoint({
        binding,
        roots: this.os.manifest.ownedRoots,
        databases: this.os.manifest.linux.databases,
        controlDirectory: this.os.manifest.controlDirectory,
        journal,
        audit: {
          manifest: this.os.manifest,
          identity: this.os.instance(),
          state: this.control.read(),
          ledger: this.control.ledger(),
          managedIpc: this.read("quiesced.json"),
        },
        gitRefs: fs.refs,
      });
      this.stopped(binding);
      this.save("final-checkpoint.json", cp);
      encrypted = encryptCheckpoint(cp, prepared.controllerPublicKeyPem);
    }
    if (cap) await uploadCheckpoint(this.os.manifest, encrypted, cap);
    return {
      checkpointId: cp.checkpointId,
      manifestDigest: cp.manifestDigest,
      ciphertextSha256: encrypted.ciphertextSha256,
      ciphertextBytes: encrypted.ciphertextBytes,
      uploaded: !!cap,
    };
  }
  acknowledge(
    binding: FinalBinding,
    receipt: SignedCustodyReceipt,
  ): AzureRemovalSeal {
    this.stopped(binding);
    const cp = this.read<FinalCheckpoint>("final-checkpoint.json");
    if (
      workerDigest(cp.binding) !== workerDigest(binding) ||
      stoppedSourceDigest(
        this.os.manifest.ownedRoots,
        this.os.manifest.linux.databases,
      ) !== cp.sourceDigest
    )
      fail();
    const prepared = this.read<{ controllerPublicKeyPem: string }>(
      "enrollment.json",
    );
    const encrypted = encryptCheckpoint(cp, prepared.controllerPublicKeyPem);
    verifyCustodyReceipt(receipt, encrypted, prepared.controllerPublicKeyPem);
    this.save("final-custody-receipt.json", receipt);
    this.stopped(binding);
    if (
      stoppedSourceDigest(
        this.os.manifest.ownedRoots,
        this.os.manifest.linux.databases,
      ) !== cp.sourceDigest
    )
      fail();
    const state = this.state(binding);
    const seal: AzureRemovalSeal = {
      workerId: binding.workerId,
      vmId: binding.vmId,
      operationId: binding.removalOperationId,
      observedAt: Date.now(),
      drainConfirmedAt: state.drainStartedAt!,
      drainGeneration: String(binding.drainGeneration),
      beginDrainConfirmed: true,
      fenced: true,
      active: 0,
      claims: 0,
      processes: 0,
      pendingTransfers: false,
      pendingActivationIntents: false,
      coordinatorCheckpointAcknowledged: true,
      clean: true,
      pendingHumanReturns: false,
    };
    this.save("seal.json", seal);
    return seal;
  }
  finalStatus(input: unknown) {
    const request = finalStatusRequestSchema.parse(input);
    const seal = this.status(request.binding);
    if (!seal) fail();
    return finalStatusReceiptSchema.parse({ ...request, seal,
      checkpointReceiptDigest: workerDigest(this.read("final-custody-receipt.json")) });
  }
  status(binding: FinalBinding): AzureRemovalSeal | null {
    this.stopped(binding);
    if (!existsSync(this.path("seal.json"))) return null;
    return this.acknowledge(binding, this.read("final-custody-receipt.json"));
  }
}
