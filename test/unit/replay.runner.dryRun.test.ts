// test/unit/replay.runner.dryRun.test.ts
import { describe, expect, it } from "bun:test";
import { runReplayBatchDryRun } from "../../src/replay/runner.ts";
import type { EventProducer } from "../../src/kafka/producer.ts";
import type { DlqRecord } from "../../src/replay/types.ts";

function rec(offset: number): DlqRecord {
  return {
    topic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    partition: 0,
    offset,
    key: Buffer.from("k"),
    value: Buffer.from("v"),
    headers: [],
    timestamp: Date.now(),
  };
}

async function* records(n: number, start = 0): AsyncIterable<DlqRecord> {
  for (let i = 0; i < n; i++) yield rec(start + i);
}

function makeSpyProducer(): { producer: EventProducer; calls: number } {
  let calls = 0;
  const producer: EventProducer = {
    sendByTopic: async () => {
      calls += 1;
    },
    isConnected: () => true,
    disconnect: async () => {},
  };
  return {
    producer,
    get calls() {
      return calls;
    },
  };
}

describe("runReplayBatchDryRun", () => {
  it("counts scanned records and tracks last offset", async () => {
    const spy = makeSpyProducer();
    const result = await runReplayBatchDryRun({
      records: records(5),
      producer: spy.producer,
    });
    expect(result.scanned).toBe(5);
    expect(result.replayed).toBe(0);
    expect(result.parked).toBe(0);
    expect(result.skipped).toBe(0);
    expect(result.errors).toBe(0);
    expect(result.lastOffset).toBe(4);
  });

  it("never calls the producer (dry-run invariant)", async () => {
    const spy = makeSpyProducer();
    await runReplayBatchDryRun({
      records: records(10),
      producer: spy.producer,
    });
    expect(spy.calls).toBe(0);
  });

  it("returns lastOffset=null for an empty stream", async () => {
    const spy = makeSpyProducer();
    const result = await runReplayBatchDryRun({
      records: records(0),
      producer: spy.producer,
    });
    expect(result.scanned).toBe(0);
    expect(result.lastOffset).toBeNull();
  });

  it("preserves the highest offset seen, regardless of start", async () => {
    const spy = makeSpyProducer();
    const result = await runReplayBatchDryRun({
      records: records(3, 100),
      producer: spy.producer,
    });
    expect(result.scanned).toBe(3);
    expect(result.lastOffset).toBe(102);
  });
});
