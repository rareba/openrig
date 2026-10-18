import "@testing-library/jest-dom/vitest";
import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { HumanResponse } from "../src/components/for-you/HumanResponse.js";

const questions = [{ id: "release", question: "Release now?", options: [
  { id: "yes", label: "Release", recommended: true, consequences: "Customers receive this version." },
  { id: "no", label: "Wait", consequences: "The current version stays available." },
] }];
function show() {
  return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
    <HumanResponse qitemId="request-1" destinationSession="operator@external" questions={questions} answers={{}} requestRevision="revision-1" hostId="vps-b" />
  </QueryClientProvider>);
}
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); });
describe("human response choices", () => {
  it("shows recommendations and consequences and requires explicit operator identity", () => {
    show();
    expect(screen.getByText("Customers receive this version.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Release \(Recommended\)/ })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Ask a question" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Write a custom response" })).toBeInTheDocument();
  });
  it("sends clarification explicitly and retains the pending decision", async () => {
    const fetcher = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ replyQitemId: "reply-1", complete: false }) });
    vi.stubGlobal("fetch", fetcher);
    localStorage.setItem("openrig.missionControlBearerToken", "local-token");
    show();
    fireEvent.change(screen.getByLabelText("Operator identity"), { target: { value: "operator@external" } });
    fireEvent.click(screen.getByRole("button", { name: "Ask a question" }));
    fireEvent.change(screen.getByLabelText("Your clarification question"), { target: { value: "Which users?" } });
    fireEvent.click(screen.getByRole("button", { name: "Send question" }));
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("/api/mission-control/human-response");
    expect(init.headers.Authorization).toBe("Bearer local-token");
    expect(JSON.parse(init.body)).toMatchObject({ qitemId: "request-1", actorSession: "operator@external", hostId: "vps-b", requestRevision: "revision-1", response: { kind: "clarification", text: "Which users?" } });
    expect(await screen.findByRole("status")).toHaveTextContent("The decision remains pending.");
  });
  it("retains the same reply ID for a failed submission retry", async () => {
    const fetcher = vi.fn().mockRejectedValueOnce(new Error("Connection lost"))
      .mockResolvedValue({ ok: true, json: async () => ({ replyQitemId: "reply-1", complete: true }) });
    vi.stubGlobal("fetch", fetcher);
    show();
    fireEvent.change(screen.getByLabelText("Operator identity"), { target: { value: "operator@external" } });
    fireEvent.click(screen.getByRole("button", { name: /Release \(Recommended\)/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Connection lost");
    fireEvent.click(screen.getByRole("button", { name: /Release \(Recommended\)/ }));
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(2));
    expect(JSON.parse(fetcher.mock.calls[0][1].body).replyId).toBe(JSON.parse(fetcher.mock.calls[1][1].body).replyId);
  });
  it("keeps the existing operator context and displays a request identity mismatch", async () => {
    const fetcher = vi.fn().mockResolvedValue({ ok: false, json: async () => ({ message: "Only the asked human may respond to this request." }) });
    vi.stubGlobal("fetch", fetcher);
    render(<QueryClientProvider client={new QueryClient()}><HumanResponse qitemId="request-2" destinationSession="human-other@host" actorSession="human@kernel" questions={questions} answers={{}} requestRevision="revision-2" /></QueryClientProvider>);
    expect(screen.queryByLabelText("Operator identity")).toBeNull();
    expect(screen.getByText("This request is addressed to human-other@host.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Release \(Recommended\)/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Only the asked human");
    expect(JSON.parse(fetcher.mock.calls[0][1].body).actorSession).toBe("human@kernel");
  });

  it("records forwarding without claiming manager delivery and locks partial responses until reconciliation", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({ replyQitemId: "passive-receipt", complete: false, forwardingStatus: "pending" }) }));
    show();
    fireEvent.change(screen.getByLabelText("Operator identity"), { target: { value: "operator@external" } });
    fireEvent.click(screen.getByRole("button", { name: /Release \(Recommended\)/ }));
    expect(await screen.findByRole("status")).toHaveTextContent("recorded for forwarding. Origin delivery is pending.");
    expect(screen.getByRole("button", { name: "Ask a question" })).toBeDisabled();
    expect(screen.queryByText(/sent to the manager/)).toBeNull();
  });
  it("keeps a reloaded pending-forward proxy disabled and truthful", () => {
    render(<QueryClientProvider client={new QueryClient()}><HumanResponse qitemId="proxy" destinationSession="human@kernel" actorSession="human@kernel" questions={questions} answers={{}} requestRevision="revision" forwardingStatus="pending" /></QueryClientProvider>);
    expect(screen.getByRole("status")).toHaveTextContent("Origin delivery is pending.");
    expect(screen.getByRole("button", { name: "Ask a question" })).toBeDisabled();
  });

});
