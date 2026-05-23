// test/unit/replay.scheduler.test.ts
import { describe, expect, it } from "bun:test";
import type { ReplayConfig, RouteConfig } from "../../src/config/schemas.ts";
import type { EventProducer } from "../../src/kafka/producer.ts";
import type { ReplayConsumer } from "../../src/replay/consumer.ts";
import type {
  DlqDepthCache,
  PartitionDepth,
} from "../../src/replay/dlqInspector.ts";
import { createReplayJobStore } from "../../src/replay/jobStore.ts";
import { createReplayScheduler } from "../../src/replay/scheduler.ts";
import type { DlqRecord, HeaderTuple } from "../../src/replay/types.ts";

const TRANSIENT = "org.apache.kafka.common.errors.RetriableException";

function h(name: string, val: string): HeaderTuple {
  return [Buffer.from(name), Buffer.from(val)];
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
  poisonErrors: [],
  default: "park",
  maxRecordsPerJob: 100,
  rateLimitPerSec: 1_000_000,
  parkingTopicSuffix: ".parked",
  auto: {
    enabled: true,
    intervalMs: 60_000,
    dlqDepthThreshold: 10,
    probeWindowRecords: 500,
  },
};

function fakeDepthCache(values: Map<string, PartitionDepth[]>): DlqDepthCache {
  return {
    get: (route) => values.get(route) ?? [],
    refresh: async () => {},
    start: () => {},
    stop: () => {},
  };
}

function makeProducer() {
  const sent: string[] = [];
  const producer: EventProducer = {
    sendByTopic: async (topic) => {
      sent.push(topic);
    },
    isConnected: () => true,
    disconnect: async () => {},
  };
  return { producer, sent };
}

function rec(partition: number, offset: number): DlqRecord {
  return {
    topic: ROUTE.dlqTopic,
    partition,
    offset,
    key: Buffer.from(`k${offset}`),
    value: Buffer.from(`v${offset}`),
    headers: [h("__connect.errors.exception.class.name", TRANSIENT)],
    timestamp: Date.now(),
  };
}

function fakeConsumerFactory(records: DlqRecord[]) {
  return async (
    _route: RouteConfig,
    _jobId: string,
  ): Promise<ReplayConsumer> => {
    return {
      groupId: "g",
      async fetchOne() {
        return null;
      },
      async *streamRange(opts) {
        for (const r of records) {
          if (r.partition !== opts.partition) continue;
          if (opts.signal.aborted) break;
          yield r;
        }
      },
      async close() {},
    };
  };
}

describe("createReplayScheduler", () => {
  it("runs auto replay when (route, partition) depth >= threshold", async () => {
    const depths = new Map<string, PartitionDepth[]>([
      [
        ROUTE.name,
        [
          {
            partition: 0,
            depth: 50,
            observedAt: Date.now(),
            ageMs: 0,
          },
        ],
      ],
    ]);
    const { producer, sent } = makeProducer();
    const jobStore = createReplayJobStore();
    const scheduler = createReplayScheduler({
      cfg: CFG,
      routes: [ROUTE],
      dlqDepth: fakeDepthCache(depths),
      jobStore,
      producer,
      createConsumer: fakeConsumerFactory([rec(0, 1), rec(0, 2)]),
    });
    await scheduler.tick();
    expect(sent).toHaveLength(2);
    expect(sent[0]).toBe(ROUTE.topic);
    // Watermark advanced past last replayed offset.
    expect(jobStore.getLastReplayedOffset(ROUTE.name, 0)).toBe(2);
  });

  it("skips partitions below threshold", async () => {
    const depths = new Map<string, PartitionDepth[]>([
      [
        ROUTE.name,
        [
          {
            partition: 0,
            depth: 5,
            observedAt: Date.now(),
            ageMs: 0,
          },
        ],
      ],
    ]);
    const { producer, sent } = makeProducer();
    const jobStore = createReplayJobStore();
    const scheduler = createReplayScheduler({
      cfg: CFG,
      routes: [ROUTE],
      dlqDepth: fakeDepthCache(depths),
      jobStore,
      producer,
      createConsumer: fakeConsumerFactory([rec(0, 1)]),
    });
    await scheduler.tick();
    expect(sent).toHaveLength(0);
  });

  it("skips partitions whose depth is null (probe failed)", async () => {
    const depths = new Map<string, PartitionDepth[]>([
      [
        ROUTE.name,
        [
          {
            partition: 0,
            depth: null,
            observedAt: Date.now(),
            ageMs: 0,
            depthError: "UnsupportedVersion",
          },
        ],
      ],
    ]);
    const { producer, sent } = makeProducer();
    const jobStore = createReplayJobStore();
    const scheduler = createReplayScheduler({
      cfg: CFG,
      routes: [ROUTE],
      dlqDepth: fakeDepthCache(depths),
      jobStore,
      producer,
      createConsumer: fakeConsumerFactory([rec(0, 1)]),
    });
    await scheduler.tick();
    expect(sent).toHaveLength(0);
  });

  it("does not start a new job when one is already active for the same partition", async () => {
    const depths = new Map<string, PartitionDepth[]>([
      [
        ROUTE.name,
        [
          {
            partition: 0,
            depth: 100,
            observedAt: Date.now(),
            ageMs: 0,
          },
        ],
      ],
    ]);
    const { producer, sent } = makeProducer();
    const jobStore = createReplayJobStore();
    // Seed an active job so hasActiveJob returns true.
    jobStore.create({ route: ROUTE.name, partition: 0, mode: "manual", dryRun: false });
    const scheduler = createReplayScheduler({
      cfg: CFG,
      routes: [ROUTE],
      dlqDepth: fakeDepthCache(depths),
      jobStore,
      producer,
      createConsumer: fakeConsumerFactory([rec(0, 1)]),
    });
    await scheduler.tick();
    expect(sent).toHaveLength(0);
  });

  it("resume picks up from the watermark + 1", async () => {
    const depths = new Map<string, PartitionDepth[]>([
      [
        ROUTE.name,
        [
          {
            partition: 0,
            depth: 100,
            observedAt: Date.now(),
            ageMs: 0,
          },
        ],
      ],
    ]);
    const { producer, sent } = makeProducer();
    const jobStore = createReplayJobStore();
    jobStore.setLastReplayedOffset(ROUTE.name, 0, 5);
    // Records contain offsets 1..10; with watermark=5, fromOffset=6, so only
    // offsets 6,7,8,9,10 should replay.
    const records: DlqRecord[] = [];
    for (let o = 1; o <= 10; o++) records.push(rec(0, o));
    // The consumer's streamRange in our fake doesn't actually honour
    // fromOffset (it yields everything regardless of opts), so this test
    // verifies the scheduler PASSES fromOffset=6 to streamRange — captured
    // below.
    let observedFromOffset = -1;
    const createConsumer = async (): Promise<ReplayConsumer> => ({
      groupId: "g",
      async fetchOne() {
        return null;
      },
      async *streamRange(opts) {
        observedFromOffset = opts.fromOffset;
        for (const r of records) {
          if (r.partition !== opts.partition) continue;
          if (r.offset < opts.fromOffset) continue;
          if (opts.signal.aborted) break;
          yield r;
        }
      },
      async close() {},
    });
    const scheduler = createReplayScheduler({
      cfg: CFG,
      routes: [ROUTE],
      dlqDepth: fakeDepthCache(depths),
      jobStore,
      producer,
      createConsumer,
    });
    await scheduler.tick();
    expect(observedFromOffset).toBe(6);
    expect(sent).toHaveLength(5); // offsets 6..10
  });

  it("tick early-returns when a previous tick is still in flight", async () => {
    const depths = new Map<string, PartitionDepth[]>([
      [
        ROUTE.name,
        [
          {
            partition: 0,
            depth: 100,
            observedAt: Date.now(),
            ageMs: 0,
          },
        ],
      ],
    ]);
    const { producer, sent } = makeProducer();
    const jobStore = createReplayJobStore();
    let invocations = 0;
    const createConsumer = async (): Promise<ReplayConsumer> => {
      invocations += 1;
      return {
        groupId: "g",
        async fetchOne() {
          return null;
        },
        async *streamRange() {
          // Block until released externally; lets us run an overlapping tick.
          await new Promise((resolve) => setTimeout(resolve, 50));
        },
        async close() {},
      };
    };
    const scheduler = createReplayScheduler({
      cfg: CFG,
      routes: [ROUTE],
      dlqDepth: fakeDepthCache(depths),
      jobStore,
      producer,
      createConsumer,
    });
    const tick1 = scheduler.tick();
    // Immediate second tick — must be a no-op because tickInFlight is true.
    await scheduler.tick();
    await tick1;
    expect(invocations).toBe(1);
    expect(sent).toHaveLength(0);
  });
});
