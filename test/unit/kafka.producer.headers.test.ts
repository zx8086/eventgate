// test/unit/kafka.producer.headers.test.ts
import { describe, expect, it } from "bun:test";
import { normalizeHeaders } from "../../src/kafka/producer.ts";

describe("normalizeHeaders", () => {
  it("returns undefined for null and undefined input", () => {
    expect(normalizeHeaders(undefined)).toBeUndefined();
    expect(normalizeHeaders(null)).toBeUndefined();
  });

  it("converts Record<string,string> to Map<Buffer,Buffer>", () => {
    const out = normalizeHeaders({ a: "1", b: "2" });
    expect(out).toBeInstanceOf(Map);
    const found: Record<string, string> = {};
    for (const [k, v] of out as Map<Buffer, Buffer>) {
      found[k.toString("utf-8")] = v.toString("utf-8");
    }
    expect(found).toEqual({ a: "1", b: "2" });
  });

  it("converts array form with string values", () => {
    const out = normalizeHeaders([
      ["foo", "bar"],
      ["baz", "qux"],
    ]) as Map<Buffer, Buffer>;
    expect(out.size).toBe(2);
    const found: Record<string, string> = {};
    for (const [k, v] of out) found[k.toString("utf-8")] = v.toString("utf-8");
    expect(found).toEqual({ foo: "bar", baz: "qux" });
  });

  it("preserves Buffer values bit-for-bit (binary idempotencyKey)", () => {
    const binary = Buffer.from([0x00, 0xff, 0x10, 0x42, 0x80, 0x01]);
    const out = normalizeHeaders([["idempotencyKey", binary]]) as Map<
      Buffer,
      Buffer
    >;
    let preserved: Buffer | undefined;
    for (const [k, v] of out) {
      if (k.toString("utf-8") === "idempotencyKey") preserved = v;
    }
    expect(preserved?.equals(binary)).toBe(true);
  });

  it("skips array tuples whose value is null", () => {
    const out = normalizeHeaders([
      ["keep", "yes"],
      ["drop", null],
    ]) as Map<Buffer, Buffer>;
    expect(out.size).toBe(1);
    const first = [...out.entries()][0];
    expect(first?.[0].toString("utf-8")).toBe("keep");
  });
});
