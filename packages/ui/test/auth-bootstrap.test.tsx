import { expect, it, vi } from "vitest";
import { missionControlAuthHeaders, terminalAuthHeaders } from "../src/components/mission-control/missionControlAuth.js";

const { render } = vi.hoisted(() => ({ render: vi.fn() }));
vi.mock("react-dom/client", () => ({ createRoot: () => ({ render }) }));
vi.mock("../src/App.js", () => ({ App: () => null }));

it("installs and removes URL credentials before rendering protected human controls", async () => {
  localStorage.clear();
  history.replaceState(null, "", "/for-you?mcToken=test-operator&termToken=test-terminal&view=needs-you#question");
  document.body.innerHTML = '<div id="root"></div>';
  render.mockImplementation(() => {
    expect(missionControlAuthHeaders()).toEqual({ Authorization: "Bearer test-operator" });
    expect(terminalAuthHeaders()).toEqual({ Authorization: "Bearer test-terminal" });
    expect(location.search).toBe("?view=needs-you");
    expect(location.hash).toBe("#question");
  });
  try {
    await import("../src/main.js");
    expect(render).toHaveBeenCalledOnce();
  } finally {
    localStorage.clear();
    history.replaceState(null, "", "/");
    document.body.innerHTML = "";
  }
});
