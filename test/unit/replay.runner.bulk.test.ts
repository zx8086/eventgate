// test/unit/replay.runner.bulk.test.ts
import { describe, expect, it } from "bun:test";
import type { ReplayConfig, RouteConfig } from "../../src/config/schemas.ts";
import type { EventProducer, ProducerHeaders } from "../../src/kafka/producer.ts";
import { runReplayBatch } from "../../src/replay/runner.ts";
import type { DlqRecord, HeaderTuple } from "../../src/replay/types.ts";
import { CircuitBreakerOpenError } from "../../src/resilience/errors.ts";

const TRANSIENT = "org.apache.kafka.common.errors.RetriableException";
const POISON = "org.apache.kafka.connect.errors.DataException";

function h(name: string, val: string): HeaderTuple {
  return [Buffer.from(name), Buffer.from(val)];
}

function rec(
  offset: number,
  excClass?: string,
  extra: HeaderTuple[] = [],
): DlqRecord {
  const headers: HeaderTuple[] = [...extra];
  if (excClass !== undefined) {
    headers.push(h("__connect.errors.exception.class.name", excClass));
  }
  return {
    topic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    partition: 0,
    offset,
    key: Buffer.from(`k${offset}`),
    value: Buffer.from(`v${offset}`),
    headers,
    timestamp: Date.now(),
  };
}

async function* asyncIter(items: DlqRecord[]): AsyncIterable<DlqRecord> {
  for (const i of items) yield i;
}

const ROUTE: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  sourceHeader: "elastic-autoops",
  keyFields: ["resourceId"],
  idempotency: "elastic-autoops",
};

const CFG: ReplayConfig = {
  enabled: true,
  maxAttempts: 5,
  transientErrors: [TRANSIENT],
  poisonErrors: [POISON],
  default: "park",
  maxRecordsPerJob: 100,
  rateLimitPerSec: 1_000_000, // effectively unthrottled for tests
  parkingTopicSuffix: ".parked",
  auto: {
    enabled: false,
    intervalMs: 300_000,
    dlqDepthThreshold: 100,
    probeWindowRecords: 500,
  },
};

type Sent = {
  topic: string;
  key: string;
  value: string;
  headers?: ProducerHeaders;
};

function makeProducer(opts: { breakerOpensAfter?: number; throwAfter?: number } = {}) {
  const sent: Sent[] = [];
  let count = 0;
  const producer: EventProducer = {
    sendByTopic: async (topic, key, value, headers) => {
      count += 1;
      if (opts.breakerOpensAfter !== undefined && count > opts.breakerOpensAfter) {
        throw new CircuitBreakerOpenError(new Date(Date.now() + 60_000));
      }
      if (opts.throwAfter !== undefined && count > opts.throwAfter) {
        throw new Error("transport boom");
      }
      sent.push({ topic, key, value, headers });
    },
    isConnected: () => true,
    disconnect: async () => {},
  };
  return { producer, sent };
}

describe("runReplayBatch", () => {
  it("replays transient records to the source topic, parks poison to the parking topic", async () => {
    const { producer, sent } = makeProducer();
    const ctl = new AbortController();
    const result = await runReplayBatch({
      records: asyncIter([rec(1, TRANSIENT), rec(2, POISON), rec(3, TRANSIENT)]),
      producer,
      cfg: CFG,
      jobId: "job-1",
      route: ROUTE,
      signal: ctl.signal,
    });
    expect(result.scanned).toBe(3);
    expect(result.replayed).toBe(2);
    expect(result.parked).toBe(1);
    expect(result.errors).toBe(0);
    expect(result.paused).toBe(false);
    expect(result.lastOffset).toBe(3);

    const topics = sent.map((s) => s.topic);
    expect(topics).toEqual([
      "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS.parked",
      "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    ]);
  });

  it("with empty parkingTopicSuffix, park is count-only (no produce)", async () => {
    const { producer, sent } = makeProducer();
    const ctl = new AbortController();
    const cfg: ReplayConfig = { ...CFG, parkingTopicSuffix: "" };
    const result = await runReplayBatch({
      records: asyncIter([rec(1, POISON), rec(2, POISON)]),
      producer,
      cfg,
      jobId: "job-2",
      route: ROUTE,
      signal: ctl.signal,
    });
    expect(result.parked).toBe(2);
    expect(result.replayed).toBe(0);
    expect(sent).toHaveLength(0);
  });

  it("pauses on CircuitBreakerOpenError without bumping errors/attempts", async () => {
    // 2 records, breaker opens after the first successful produce.
    const { producer, sent } = makeProducer({ breakerOpensAfter: 1 });
    const ctl = new AbortController();
    const result = await runReplayBatch({
      records: asyncIter([rec(1, TRANSIENT), rec(2, TRANSIENT)]),
      producer,
      cfg: CFG,
      jobId: "job-3",
      route: ROUTE,
      signal: ctl.signal,
    });
    expect(result.paused).toBe(true);
    expect(result.lastError).toBe("circuit_breaker_open");
    expect(result.nextResumeAt).not.toBeNull();
    expect(result.replayed).toBe(1);
    expect(result.errors).toBe(0); // breaker pause is NOT an error per spec
    expect(result.lastOffset).toBe(2); // points at the record that triggered the breaker
    expect(sent).toHaveLength(1);
  });

  it("counts non-breaker produce errors and continues", async () => {
    const { producer, sent } = makeProducer({ throwAfter: 1 });
    const ctl = new AbortController();
    const result = await runReplayBatch({
      records: asyncIter([rec(1, TRANSIENT), rec(2, TRANSIENT), rec(3, TRANSIENT)]),
      producer,
      cfg: CFG,
      jobId: "job-4",
      route: ROUTE,
      signal: ctl.signal,
    });
    expect(result.replayed).toBe(1);
    expect(result.errors).toBe(2);
    expect(result.paused).toBe(false);
    expect(sent).toHaveLength(1);
  });

  it("emits onProgress after every record (counter snapshots)", async () => {
    const { producer } = makeProducer();
    const ctl = new AbortController();
    const snapshots: Array<{ scanned: number; replayed: number }> = [];
    await runReplayBatch({
      records: asyncIter([rec(1, TRANSIENT), rec(2, TRANSIENT)]),
      producer,
      cfg: CFG,
      jobId: "job-5",
      route: ROUTE,
      signal: ctl.signal,
      onProgress: (snap) => snapshots.push({ scanned: snap.scanned, replayed: snap.replayed }),
    });
    expect(snapshots.length).toBeGreaterThanOrEqual(2);
    expect(snapshots[snapshots.length - 1]).toEqual({ scanned: 2, replayed: 2 });
  });

  it("stops promptly when signal.aborted is set before the first record", async () => {
    const { producer, sent } = makeProducer();
    const ctl = new AbortController();
    ctl.abort();
    const result = await runReplayBatch({
      records: asyncIter([rec(1, TRANSIENT), rec(2, TRANSIENT)]),
      producer,
      cfg: CFG,
      jobId: "job-6",
      route: ROUTE,
      signal: ctl.signal,
    });
    expect(result.scanned).toBe(0);
    expect(sent).toHaveLength(0);
    expect(result.paused).toBe(false);
  });

  it("park decisions do NOT bump the attempt header", async () => {
    const { producer, sent } = makeProducer();
    const ctl = new AbortController();
    const recWithAttempt = rec(1, POISON, [h("x-eventgate-replay-attempt", "2")]);
    await runReplayBatch({
      records: asyncIter([recWithAttempt]),
      producer,
      cfg: CFG,
      jobId: "job-7",
      route: ROUTE,
      signal: ctl.signal,
    });
    expect(sent).toHaveLength(1);
    const headers = sent[0]?.headers as Array<[string, string | Buffer | null]>;
    const byName = Object.fromEntries(
      headers.map(([k, v]) => [k, v instanceof Buffer ? v.toString("utf-8") : v]),
    );
    // Spec §"On park": no attempt bump. Original attempt=2 is preserved.
    expect(byName["x-eventgate-replay-attempt"]).toBe("2");
  });

  it("replay decisions bump the attempt header by 1", async () => {
    const { producer, sent } = makeProducer();
    const ctl = new AbortController();
    const recWithAttempt = rec(1, TRANSIENT, [h("x-eventgate-replay-attempt", "2")]);
    await runReplayBatch({
      records: asyncIter([recWithAttempt]),
      producer,
      cfg: CFG,
      jobId: "job-8",
      route: ROUTE,
      signal: ctl.signal,
    });
    const headers = sent[0]?.headers as Array<[string, string | Buffer | null]>;
    const byName = Object.fromEntries(
      headers.map(([k, v]) => [k, v instanceof Buffer ? v.toString("utf-8") : v]),
    );
    expect(byName["x-eventgate-replay-attempt"]).toBe("3");
  });
});
