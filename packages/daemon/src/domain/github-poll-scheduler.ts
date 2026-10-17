interface PollService {
  status(): { config: { pollIntervalSeconds?: number } | null };
  reconcile(): Promise<{ error: string | null }>;
}

/** Polling is ordinary code; an unchanged backlog never requires an agent turn. */
export class PollScheduler {
  private stopped = true;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private running: Promise<void> | undefined;
  private lastError: string | null = null;
  constructor(private service: PollService, private report = (message: string) => console.error(message)) {}
  start() {
    if (!this.stopped) return;
    this.stopped = false;
    this.poll();
  }
  private poll() {
    if (this.stopped) return;
    this.running = this.service.reconcile().then(result => {
      if (result.error && result.error !== this.lastError) this.report(result.error);
      this.lastError = result.error;
    }).catch(error => {
      const message = error instanceof Error ? error.message : String(error);
      if (message !== this.lastError) this.report(message);
      this.lastError = message;
    }).finally(() => {
      this.running = undefined;
      if (!this.stopped) this.timer = setTimeout(() => this.poll(), (this.service.status().config?.pollIntervalSeconds ?? 60) * 1000);
    });
  }
  async stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    await this.running;
  }
}
