import { describe, expect, it } from "vitest";
import { createProgram } from "../src/index.js";

describe("GitHub intake CLI", () => {
  it("exposes configuration, reconciliation, eligibility and status on the native CLI", () => {
    const command = createProgram().commands.find((entry) => entry.name() === "github");
    expect(command, "GitHub intake must be operable without a browser").toBeDefined();
    expect(command!.commands.map((entry) => entry.name()).sort()).toEqual(["configure", "eligibility", "status", "sync"]);
  });
});
