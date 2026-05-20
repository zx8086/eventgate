// test/unit/config.reservedPaths.test.ts
import { describe, expect, it } from "bun:test";
import { RESERVED_PATHS, checkReservedPath, isReservedPath } from "../../src/config/reservedPaths.ts";

describe("RESERVED_PATHS", () => {
  it("contains /healthz", () => {
    expect(RESERVED_PATHS.has("/healthz")).toBe(true);
  });
  it("contains /admin/routes", () => {
    expect(RESERVED_PATHS.has("/admin/routes")).toBe(true);
  });
});

describe("isReservedPath", () => {
  it("returns true for reserved paths", () => {
    expect(isReservedPath("/healthz")).toBe(true);
    expect(isReservedPath("/admin/routes")).toBe(true);
  });
  it("returns false for non-reserved paths", () => {
    expect(isReservedPath("/webhooks/elastic/autoops")).toBe(false);
    expect(isReservedPath("/admin")).toBe(false);
    expect(isReservedPath("/admin/routes/x")).toBe(false);
  });
});

describe("checkReservedPath", () => {
  it("returns ok for a non-reserved path", () => {
    expect(checkReservedPath("/webhooks/datadog")).toEqual({ ok: true });
  });
  it("returns a distinct message for /healthz", () => {
    const r = checkReservedPath("/healthz");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/'\/healthz' is reserved/);
  });
  it("returns a distinct message for /admin/routes", () => {
    const r = checkReservedPath("/admin/routes");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/'\/admin\/routes' is reserved/);
  });
});
