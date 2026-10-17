import { describe, it, expect, vi, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { queueItemOriginKey, useQueueItemMap } from "../src/hooks/useSlices.js";

afterEach(() => vi.unstubAllGlobals());
describe("queue item origin hydration", () => {
  it("reads colliding request IDs separately at their explicit origins", async () => {
    const fetcher = vi.fn().mockImplementation(async (url: string) => ({ ok: true, status: 200,
      json: async () => ({ qitemId: "same-id", body: url.includes("host=") ? "Remote question" : "Local question" }) }));
    vi.stubGlobal("fetch", fetcher);
    const refs = [{ qitemId: "same-id", hostId: "local" }, { qitemId: "same-id", hostId: "vps-b" }];
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHook(() => useQueueItemMap(refs), { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    await waitFor(() => expect(result.current.itemsById.size).toBe(2));
    expect(result.current.itemsById.get(queueItemOriginKey("same-id", "local"))?.body).toBe("Local question");
    expect(result.current.itemsById.get(queueItemOriginKey("same-id", "vps-b"))?.body).toBe("Remote question");
    expect(fetcher.mock.calls.map(([url]) => url).sort()).toEqual(["/api/queue/same-id", "/api/queue/same-id?host=vps-b"]);
  });
});
