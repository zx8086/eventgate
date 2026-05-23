// test/unit/config.reservedPaths.replay.test.ts
import { describe, expect, it } from "bun:test";
import {
  RESERVED_PATHS,
  checkReservedPath,
  isReservedPath,
} from "../../src/config/reservedPaths.ts";

describe("RESERVED_PATHS (replay additions)", () => {
  it("contains /admin/dlq", () => {
    expect(RESERVED_PATHS.has("/admin/dlq")).toBe(true);
  });
  it("contains /admin/replay", () => {
    expect(RESERVED_PATHS.has("/admin/replay")).toBe(true);
  });
});

describe("isReservedPath (replay prefixes)", () => {
  it.each([
    ["/admin/replay"],
    ["/admin/replay/elastic-autoops"],
    ["/admin/replay/elastic-autoops/message"],
    ["/admin/replay/some-uuid-string/cancel"],
    ["/admin/dlq"],
    ["/admin/dlq/anything"],
  ])("treats %s as reserved", (path) => {
    expect(isReservedPath(path)).toBe(true);
  });

  it.each([
    ["/admin/replays"],
    ["/admin/replaymigrations"],
    ["/admin/dlqs"],
    ["/admin"],
    ["/webhooks/elastic/autoops"],
  ])("does NOT treat %s as reserved", (path) => {
    expect(isReservedPath(path)).toBe(false);
  });
});

describe("checkReservedPath", () => {
  it("rejects /admin/replay with a distinct message", () => {
    const r = checkReservedPath("/admin/replay");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/'\/admin\/replay' is reserved/);
  });
  it("rejects a parametric sub-path with the full path in the message", () => {
    const r = checkReservedPath("/admin/replay/foo/cancel");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/'\/admin\/replay\/foo\/cancel' is reserved/);
  });
  it("permits unrelated paths", () => {
    expect(checkReservedPath("/webhooks/datadog/alerts").ok).toBe(true);
  });
});
