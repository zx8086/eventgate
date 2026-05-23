// src/replay/consumer.ts
import { Consumer } from "@platformatic/kafka";
import type { RouteConfig } from "../config/schemas.ts";
import type { KafkaProvider } from "../kafka/providers/index.ts";
import { getLogger } from "../logging/index.ts";
import type { DlqRecord, HeaderTuple } from "./types.ts";

// Optional fire-and-forget consumer-group cleanup. Wired by the gateway to
// healthAdmin.deleteGroups (reuses the existing long-lived Admin client).
// Failures are warn-logged but never propagate — group cleanup is hygiene,
// not correctness.
export type GroupCleanup = (groupId: string) => Promise<void>;

const log = getLogger("replay.consumer");

export type StreamRangeOpts = {
  partition: number;
  fromOffset: number;
  // inclusive; when undefined, maxRecords is the only cap
  toOffset?: number;
  maxRecords: number;
  signal: AbortSignal;
};

export type FetchOneOpts = {
  partition: number;
  offset: number;
};

export type ReplayConsumer = {
  readonly groupId: string;
  streamRange(opts: StreamRangeOpts): AsyncIterable<DlqRecord>;
  fetchOne(opts: FetchOneOpts): Promise<DlqRecord | null>;
  close(): Promise<void>;
};

type ConsumedMessage = {
  partition: number;
  offset: bigint;
  key: Buffer;
  value: Buffer;
  headers: Map<Buffer, Buffer>;
  timestamp: bigint;
};

// The SDK delivers headers as Map<Buffer,Buffer>. Kafka itself allows duplicate
// header names but the Map silently collapses them. For Phase 1+ we accept the
// collapse: the Connect dead-letter headers we triage on never duplicate, and
// any business-side duplicate would be an upstream bug we cannot recover from
// here either way. recordFromMessage emits the raw tuple array shape so the
// downstream readHeader path stays Buffer-equal.
function recordFromMessage(msg: ConsumedMessage, topic: string): DlqRecord {
  const headers: HeaderTuple[] = [];
  for (const [k, v] of msg.headers) headers.push([k, v]);
  return {
    topic,
    partition: msg.partition,
    offset: Number(msg.offset),
    key: msg.key,
    value: msg.value,
    headers,
    timestamp: Number(msg.timestamp),
  };
}

export async function createReplayConsumer(
  provider: KafkaProvider,
  route: RouteConfig,
  jobId: string,
  groupCleanup?: GroupCleanup,
): Promise<ReplayConsumer> {
  const conn = await provider.getConnectionConfig();
  // Per-job group id; UUIDv4-based (via jobId) so we never reuse and never
  // interfere with a real downstream consumer group. groupCleanup is invoked
  // fire-and-forget on close() so stale groups don't accumulate on the broker.
  const groupId = `eventgate-replay-${route.name}-${jobId}`;

  const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
    clientId: `${conn.clientId}-replay-${jobId}`,
    bootstrapBrokers: conn.bootstrapBrokers,
    ...(conn.sasl ? { sasl: conn.sasl } : {}),
    ...(conn.tls ? { tls: conn.tls } : {}),
    ...(conn.connectTimeout !== undefined ? { connectTimeout: conn.connectTimeout } : {}),
    ...(conn.timeout !== undefined ? { timeout: conn.timeout } : {}),
    groupId,
    autocommit: false,
  });

  log.debug({ route: route.name, jobId, groupId }, "replay consumer created");

  return {
    groupId,

    async *streamRange(opts) {
      // maxFetches caps how many fetch round-trips the SDK will issue. We size
      // it generously so a single record poll doesn't starve, and rely on our
      // own count + offset guard to stop the iterator at the requested window.
      const fetchBudget = Math.max(1, Math.ceil(opts.maxRecords / 100) + 1);
      const stream = await consumer.consume({
        topics: [route.dlqTopic],
        mode: "manual",
        offsets: [
          {
            topic: route.dlqTopic,
            partition: opts.partition,
            offset: BigInt(opts.fromOffset),
          },
        ],
        maxFetches: fetchBudget,
        autocommit: false,
      });

      let yielded = 0;
      try {
        for await (const msg of stream as AsyncIterable<ConsumedMessage>) {
          if (opts.signal.aborted) break;
          const rec = recordFromMessage(msg, route.dlqTopic);
          if (opts.toOffset !== undefined && rec.offset > opts.toOffset) break;
          yield rec;
          yielded += 1;
          if (yielded >= opts.maxRecords) break;
        }
      } finally {
        await stream.close();
      }
    },

    async fetchOne(opts) {
      const stream = await consumer.consume({
        topics: [route.dlqTopic],
        mode: "manual",
        offsets: [
          {
            topic: route.dlqTopic,
            partition: opts.partition,
            offset: BigInt(opts.offset),
          },
        ],
        // Single-record fetch: 1 round-trip is enough because the broker
        // returns records starting at the requested offset.
        maxFetches: 1,
        autocommit: false,
      });
      try {
        for await (const msg of stream as AsyncIterable<ConsumedMessage>) {
          const rec = recordFromMessage(msg, route.dlqTopic);
          if (rec.partition === opts.partition && rec.offset === opts.offset) {
            return rec;
          }
          // Broker may return earlier offsets within the same batch when the
          // requested offset falls mid-batch; keep iterating until match or
          // batch exhaustion.
        }
        return null;
      } finally {
        await stream.close();
      }
    },

    async close() {
      await consumer.close();
      // Fire-and-forget group cleanup. Warn-log on failure; never throw.
      if (groupCleanup !== undefined) {
        groupCleanup(groupId).catch((err: unknown) => {
          const message = err instanceof Error ? err.message : String(err);
          log.warn(
            { groupId, err: message },
            "replay group cleanup failed",
          );
        });
      }
    },
  };
}
