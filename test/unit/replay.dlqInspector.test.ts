// test/unit/replay.dlqInspector.test.ts
import { describe, expect, it } from "bun:test";
import type { RouteConfig } from "../../src/config/schemas.ts";
import {
  createDlqInspector,
  type AdminLike,
} from "../../src/replay/dlqInspector.ts";

const ROUTE: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  sourceHeader: "elastic-autoops",
  keyFields: ["resourceId"],
  idempotency: "elastic-autoops",
};

// Fake ClusterMetadata-shaped object — only the fields dlqInspector reads.
function metaWithPartitions(topic: string, partitionsCount: number): {
  id: string;
  brokers: Map<number, unknown>;
  controllerId: number;
  topics: Map<string, { id: string; partitions: unknown[]; partitionsCount: number; lastUpdate: number }>;
  lastUpdate: number;
} {
  return {
    id: "test",
    brokers: new Map(),
    controllerId: 0,
    topics: new Map([
      [
        topic,
        { id: "t", partitions: [], partitionsCount, lastUpdate: Date.now() },
      ],
    ]),
    lastUpdate: Date.now(),
  };
}

describe("createDlqInspector", () => {
  it("computes per-partition depth = latest - earliest", async () => {
    const admin: AdminLike = {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      metadata: async () => metaWithPartitions(ROUTE.dlqTopic, 3) as any,
      listOffsets: async ({ topics }) => {
        // Distinguish latest vs earliest by the timestamp sentinel.
        const isLatest = topics[0]?.partitions[0]?.timestamp === BigInt(-1);
        return topics.map((t) => ({
          name: t.name,
          partitions: t.partitions.map((p) => ({
            partitionIndex: p.partitionIndex,
            offset: isLatest ? BigInt(100 + p.partitionIndex) : BigInt(p.partitionIndex * 10),
          })),
        }));
      },
    };
    const inspector = createDlqInspector({
      admin,
      routes: [ROUTE],
      probeIntervalMs: 60_000,
    });
    await inspector.refresh(ROUTE.name);
    const out = inspector.get(ROUTE.name);
    expect(out).toHaveLength(3);
    // p0: 100 - 0 = 100; p1: 101 - 10 = 91; p2: 102 - 20 = 82
    expect(out[0]?.partition).toBe(0);
    expect(out[0]?.depth).toBe(100);
    expect(out[1]?.depth).toBe(91);
    expect(out[2]?.depth).toBe(82);
    for (const p of out) {
      expect(p.depthError).toBeUndefined();
      expect(p.ageMs).toBeGreaterThanOrEqual(0);
    }
  });

  it("degrades to depth=null + depthError sentinel when metadata throws", async () => {
    const admin: AdminLike = {
      metadata: async () => {
        const e = new Error("Redpanda does not implement this");
        e.name = "UnsupportedVersion";
        throw e;
      },
      listOffsets: async () => [],
    };
    const inspector = createDlqInspector({
      admin,
      routes: [ROUTE],
      probeIntervalMs: 60_000,
    });
    await inspector.refresh(ROUTE.name);
    const out = inspector.get(ROUTE.name);
    expect(out).toHaveLength(1);
    expect(out[0]?.partition).toBe(-1); // sentinel
    expect(out[0]?.depth).toBeNull();
    expect(out[0]?.depthError).toBe("UnsupportedVersion");
  });

  it("preserves last good cache when listOffsets fails on a subsequent refresh", async () => {
    let mode: "good" | "bad" = "good";
    const admin: AdminLike = {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      metadata: async () => metaWithPartitions(ROUTE.dlqTopic, 1) as any,
      listOffsets: async ({ topics }) => {
        if (mode === "bad") {
          const e = new Error("listOffsets failed");
          e.name = "BrokerError";
          throw e;
        }
        const isLatest = topics[0]?.partitions[0]?.timestamp === BigInt(-1);
        return topics.map((t) => ({
          name: t.name,
          partitions: [
            { partitionIndex: 0, offset: isLatest ? BigInt(50) : BigInt(10) },
          ],
        }));
      },
    };
    const inspector = createDlqInspector({
      admin,
      routes: [ROUTE],
      probeIntervalMs: 60_000,
    });
    await inspector.refresh(ROUTE.name);
    expect(inspector.get(ROUTE.name)[0]?.depth).toBe(40);
    mode = "bad";
    await inspector.refresh(ROUTE.name);
    const after = inspector.get(ROUTE.name);
    // Last good depth is preserved; depthError carries the failure class.
    expect(after[0]?.depth).toBe(40);
    expect(after[0]?.depthError).toBe("BrokerError");
  });

  it("get for an unknown route returns empty array", () => {
    const admin: AdminLike = {
      metadata: async () => {
        throw new Error("not called");
      },
      listOffsets: async () => [],
    };
    const inspector = createDlqInspector({
      admin,
      routes: [],
      probeIntervalMs: 60_000,
    });
    expect(inspector.get("nope")).toEqual([]);
  });
});
