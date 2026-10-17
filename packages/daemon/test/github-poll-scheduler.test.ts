import { afterEach, describe, expect, it, vi } from "vitest";
import { PollScheduler } from "../src/domain/github-poll-scheduler.js";

afterEach(() => vi.useRealTimers());
describe("GitHub poll lifecycle", () => {
  it("waits for the current poll before scheduling another and drains on stop", async () => {
    vi.useFakeTimers();
    let finish!: () => void;
    let calls = 0;
    const running = new Promise<void>(resolve => { finish = resolve; });
    const scheduler = new PollScheduler({ status: () => ({ config: { pollIntervalSeconds: 10 } }), reconcile: async () => { calls++; await running; return { error: null }; } });
    scheduler.start();
    await vi.advanceTimersByTimeAsync(30_000);
    expect(calls).toBe(1);
    let stopped = false;
    const stopping = scheduler.stop().then(() => { stopped = true; });
    expect(stopped).toBe(false);
    finish(); await stopping;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(calls).toBe(1);
  });
  it("recovers human continuations independently of a slow hourly GitHub poll", async () => {
    vi.useFakeTimers();
    let finish!: () => void;
    const source = new Promise<void>(resolve => { finish = resolve; });
    const github = new PollScheduler({ status: () => ({ config: { pollIntervalSeconds: 3600 } }), reconcile: async () => { await source; return { error: null }; } });
    let recoveries = 0;
    const human = new PollScheduler({ status: () => ({ config: { pollIntervalSeconds: 60 } }), reconcile: async () => { recoveries++; return { error: null }; } });
    github.start(); human.start();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(recoveries).toBe(3);
    await human.stop(); finish(); await github.stop();
  });
  it("runs again at the configured interval without requiring an agent turn", async () => {
    vi.useFakeTimers();
    let calls = 0;
    const scheduler = new PollScheduler({ status: () => ({ config: { pollIntervalSeconds: 10 } }), reconcile: async () => { calls++; return { error: null }; } });
    scheduler.start();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(calls).toBe(2);
    await scheduler.stop();
  });
});
