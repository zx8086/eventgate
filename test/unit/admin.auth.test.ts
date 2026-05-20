// test/unit/admin.auth.test.ts
import { describe, expect, it } from "bun:test";
import { verifyAdminToken } from "../../src/admin/auth.ts";

const TOKEN = "a".repeat(32);

describe("verifyAdminToken", () => {
  it("returns true for an exact match", () => {
    expect(verifyAdminToken(TOKEN, TOKEN)).toBe(true);
  });

  it("returns false for a mismatched token of the same length", () => {
    const wrong = "b".repeat(32);
    expect(verifyAdminToken(wrong, TOKEN)).toBe(false);
  });

  it("returns false for a token of different length", () => {
    expect(verifyAdminToken("short", TOKEN)).toBe(false);
    expect(verifyAdminToken(TOKEN + "x", TOKEN)).toBe(false);
  });

  it("returns false for undefined/empty provided", () => {
    expect(verifyAdminToken(undefined, TOKEN)).toBe(false);
    expect(verifyAdminToken("", TOKEN)).toBe(false);
  });
});
