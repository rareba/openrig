import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { FeedCard } from "../src/components/for-you/FeedCard.js";
import type { FeedCard as FeedCardModel } from "../src/lib/feed-classifier.js";
import type { QueueItemDetail } from "../src/hooks/useSlices.js";
vi.mock("../src/components/terminal/FocusedTerminal.js", () => ({ FocusedTerminal: () => null }));
vi.mock("../src/components/terminal/ProgressiveTerminal.js", () => ({ ProgressiveTerminal: () => null }));
vi.mock("../src/components/for-you/FeedCardTerminalDrill.js", () => ({ FeedCardTerminalDrill: () => null }));
const card: FeedCardModel = { id: "request-1", kind: "approval", title: "Release authority required", createdAt: "2026-10-17T00:00:00Z", receivedAt: 1,
  hostId: "vps-b", source: { seq: 1, type: "queue.created", ts: "2026-10-17T00:00:00Z", payload: { qitemId: "request-1", destinationSession: "operator@external" } } };
const item: QueueItemDetail = { qitemId: "request-1", tsCreated: "2026-10-17T00:00:00Z", tsUpdated: "2026-10-17T00:00:00Z", sourceSession: "manager@team", destinationSession: "operator@external", state: "pending", priority: "routine", tier: null, tags: null, body: "Please decide", summary: "Release authority", humanIntent: "decision", humanRequestRevision: "revision", humanQuestions: [{ id: "release", question: "Release now?", options: [{ id: "yes", label: "Release", recommended: true, consequences: "Users receive the version." }, { id: "no", label: "Wait" }] }] };
function show(queueItem?: QueueItemDetail) {
  return render(<QueryClientProvider client={new QueryClient()}><FeedCard card={card} queueItem={queueItem} /></QueryClientProvider>);
}
describe("For You native human decision integration", () => {
  it("offers native structured responses instead of generic approval", () => {
    show(item);
    expect(screen.getByLabelText("Human decision response")).toBeTruthy();
    expect(screen.getByText("Users receive the version.")).toBeTruthy();
    expect(screen.queryByTestId("mc-verb-approve")).toBeNull();
  });
  it("never offers approval before loading the current request revision", () => {
    show();
    expect(screen.getByText(/Refresh this request/)).toBeTruthy();
    expect(screen.queryByTestId("mc-verb-approve")).toBeNull();
  });
  it("uses the existing local human seat without Slack or repeated identity entry", () => {
    show({ ...item, destinationSession: "human@kernel" });
    expect(screen.getByLabelText("Human decision response")).toBeTruthy();
    expect(screen.getByText("Release (Recommended)").hasAttribute("disabled")).toBe(false);
    expect(screen.queryByLabelText("Operator identity")).toBeNull();
    expect(screen.getByText("Responding as human@kernel.")).toBeTruthy();
    expect(screen.getByText("Ask a question")).toBeTruthy();
    expect(screen.getByText("Write a custom response")).toBeTruthy();
    expect(screen.queryByTestId("mc-verb-approve")).toBeNull();
  });

  it("preserves the existing approve action on ordinary agent cards", () => {
    show({ ...item, destinationSession: "worker@team", humanIntent: null, humanQuestions: null, humanRequestRevision: undefined });
    expect(screen.getByTestId("mc-verb-approve")).toBeTruthy();
    expect(screen.queryByLabelText("Human decision response")).toBeNull();
  });

  it("records a custom answer without labeling it as approval", () => {
    show({ ...item, state: "done", closureReason: "no-follow-on", humanAnswers: {
      release: { kind: "typed-reply", text: "Do not release this version.", unansweredCount: 0 },
    } });
    expect(screen.getByText("Response recorded")).toBeTruthy();
    expect(screen.getByText(/Do not release this version/)).toBeTruthy();
    expect(screen.queryByText(/Approved by/)).toBeNull();
    expect(screen.queryByTestId("mc-verb-approve")).toBeNull();
  });

  it("records a legacy unstructured human reply without inferring approval", () => {
    show({ ...item, state: "done", closureReason: "no-follow-on", humanIntent: null, humanQuestions: null, humanAnswers: {} });
    expect(screen.getByText("Response recorded")).toBeTruthy();
    expect(screen.getByText("Human response recorded.")).toBeTruthy();
    expect(screen.queryByText(/Approved by/)).toBeNull();
  });

});
