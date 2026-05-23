// test/unit/replay.headers.test.ts
import { describe, expect, it } from "bun:test";
import {
  parseAttempt,
  readHeader,
  stampAuditHeaders,
  stripConnectHeaders,
} from "../../src/replay/headers.ts";
import type { HeaderTuple } from "../../src/replay/types.ts";

function h(name: string, val: string | Buffer | null): HeaderTuple {
  const v = val === null ? null : Buffer.isBuffer(val) ? val : Buffer.from(val);
  return [Buffer.from(name), v];
}

describe("readHeader", () => {
  it("returns value for matching name", () => {
    const rec = { headers: [h("foo", "bar"), h("baz", "qux")] };
    expect(readHeader(rec, "foo")).toBe("bar");
    expect(readHeader(rec, "baz")).toBe("qux");
  });

  it("returns undefined for missing", () => {
    expect(readHeader({ headers: [] }, "anything")).toBeUndefined();
    expect(readHeader({ headers: [h("foo", "bar")] }, "missing")).toBeUndefined();
  });

  it("returns empty string for present-but-null value", () => {
    expect(readHeader({ headers: [h("foo", null)] }, "foo")).toBe("");
  });

  it("preserves first occurrence when duplicate names exist", () => {
    const rec = { headers: [h("dup", "first"), h("dup", "second")] };
    expect(readHeader(rec, "dup")).toBe("first");
  });

  it("works with raw Buffer key headers (round-trip)", () => {
    // Headers built from raw Buffer keys (as the SDK delivers them) must match.
    const headers: HeaderTuple[] = [
      [Buffer.from("idempotencyKey"), Buffer.from("opaque-id")],
    ];
    expect(readHeader({ headers }, "idempotencyKey")).toBe("opaque-id");
  });

  it("skips tuples with null key without throwing", () => {
    const headers: HeaderTuple[] = [
      [null, Buffer.from("orphan")],
      [Buffer.from("foo"), Buffer.from("bar")],
    ];
    expect(readHeader({ headers }, "foo")).toBe("bar");
  });
});

describe("stripConnectHeaders", () => {
  it("drops every header whose key starts with __connect.errors.", () => {
    const headers = [
      h("__connect.errors.exception.class.name", "X"),
      h("__connect.errors.exception.message", "Y"),
      h("idempotencyKey", "keep-me"),
      h("__connect.errors.timestamp", "Z"),
    ];
    const out = stripConnectHeaders(headers);
    expect(out).toHaveLength(1);
    expect(out[0]?.[0]?.toString("utf-8")).toBe("idempotencyKey");
  });

  it("preserves Buffer values bit-for-bit (binary idempotencyKey)", () => {
    const binary = Buffer.from([0x00, 0xff, 0x10, 0x42, 0x80, 0x01]);
    const out = stripConnectHeaders([h("idempotencyKey", binary)]);
    expect(out[0]?.[1]?.equals(binary)).toBe(true);
  });

  it("retains tuples with null key (skips Connect filtering for them)", () => {
    const headers: HeaderTuple[] = [
      [null, Buffer.from("orphan")],
      h("foo", "bar"),
    ];
    const out = stripConnectHeaders(headers);
    expect(out).toHaveLength(2);
  });

  it("does not match a header that merely contains __connect.errors. mid-string", () => {
    const out = stripConnectHeaders([h("not-__connect.errors.foo", "keep")]);
    expect(out).toHaveLength(1);
  });
});

describe("stampAuditHeaders", () => {
  it("appends all five audit headers with the expected values", () => {
    const out = stampAuditHeaders([], {
      jobId: "job-abc",
      sourceTopic: "DLQ_T_X",
      partition: 2,
      offset: 42,
      attempt: 3,
    });
    const map = Object.fromEntries(
      out.map(([k, v]) => [k?.toString("utf-8") ?? "", v?.toString("utf-8") ?? null]),
    );
    expect(map["x-eventgate-replay-attempt"]).toBe("3");
    expect(map["x-eventgate-replay-job-id"]).toBe("job-abc");
    expect(map["x-eventgate-replay-source-topic"]).toBe("DLQ_T_X");
    expect(map["x-eventgate-replay-source-offset"]).toBe("2:42");
    expect(map["x-eventgate-replay-at"]).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/,
    );
  });

  it("appends, never replaces — existing headers survive", () => {
    const existing: HeaderTuple[] = [h("idempotencyKey", "keep")];
    const out = stampAuditHeaders(existing, {
      jobId: "j",
      sourceTopic: "t",
      partition: 0,
      offset: 0,
      attempt: 1,
    });
    expect(out.length).toBe(existing.length + 5);
    expect(out[0]?.[0]?.toString("utf-8")).toBe("idempotencyKey");
  });
});

describe("parseAttempt", () => {
  it.each([
    [undefined, 0],
    ["", 0],
    ["0", 0],
    ["3", 3],
    ["5abc", 0],
    ["-3", 0],
    ["1e6", 1_000_000],
    ["NaN", 0],
    ["3.7", 3],
    [" 5 ", 5],
  ])("parseAttempt(%p) -> %i", (input, expected) => {
    expect(parseAttempt(input)).toBe(expected);
  });
});
