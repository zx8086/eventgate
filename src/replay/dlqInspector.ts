// src/replay/dlqInspector.ts
import type { ClusterMetadata } from "@platformatic/kafka";
import type { RouteConfig } from "../config/schemas.ts";
import { getLogger } from "../logging/index.ts";

const log = getLogger("replay.dlqInspector");

export type PartitionDepth = {
  partition: number;
  depth: number | null;
  observedAt: number;
  ageMs: number;
  depthError?: string;
};

export type DlqDepthCache = {
  // Returns the cached per-partition depth list for a route. Includes stale
  // entries (with ageMs) and probe-failed entries (depth=null, depthError set).
  get(route: string): PartitionDepth[];
  // Force a refresh for one route. Resolves once the probe completes; safe
  // to call concurrently with get(). Used by the scheduler.
  refresh(route: string): Promise<void>;
  // Start the periodic refresh loop (cadence = probeIntervalMs).
  start(): void;
  stop(): void;
};

// Listed-offsets timestamp constants per Kafka protocol. -1 returns the
// LATEST offset (high-water mark); -2 returns EARLIEST. Depth =
// (latest - earliest) per partition; for DLQ inspection this is "records
// ever produced minus tombstone/compaction deletions". Combined with the
// per-partition watermark in replay_state, the operator sees both raw broker
// depth and where the last replay job stopped.
const LATEST_OFFSET = BigInt(-1);
const EARLIEST_OFFSET = BigInt(-2);

type ListOffsetsArgs = {
  topics: Array<{
    name: string;
    partitions: Array<{ partitionIndex: number; timestamp: bigint }>;
  }>;
};
type ListedOffsetsResult = Array<{
  name: string;
  partitions: Array<{ partitionIndex: number; offset: bigint }>;
}>;

// AdminLike: subset of @platformatic/kafka Admin we depend on. Keeping the
// surface narrow makes unit testing trivial (no SDK mocking dance).
export type AdminLike = {
  metadata(opts: { topics: string[] }): Promise<ClusterMetadata>;
  listOffsets(opts: ListOffsetsArgs): Promise<ListedOffsetsResult>;
};

function errorClass(err: unknown): string {
  if (err instanceof Error && err.name && err.name !== "Error") {
    return err.name.slice(0, 200);
  }
  const message = err instanceof Error ? err.message : String(err);
  return message.slice(0, 200);
}

export type CreateInspectorOpts = {
  admin: AdminLike;
  routes: ReadonlyArray<RouteConfig>;
  probeIntervalMs: number;
};

export function createDlqInspector(opts: CreateInspectorOpts): DlqDepthCache {
  const { admin, routes, probeIntervalMs } = opts;
  const cache = new Map<string, PartitionDepth[]>();
  let timer: ReturnType<typeof setInterval> | null = null;
  let stopped = false;

  async function probeRoute(route: RouteConfig): Promise<void> {
    const observedAt = Date.now();
    try {
      // Two-step probe: metadata gives us partition count, then per-partition
      // listOffsets for both earliest + latest. Subtract for depth.
      const meta = await admin.metadata({ topics: [route.dlqTopic] });
      const topicMeta = meta.topics.get(route.dlqTopic);
      if (topicMeta === undefined) {
        cache.set(route.name, []);
        return;
      }
      const partitionIndices = Array.from(
        { length: topicMeta.partitionsCount },
        (_, i) => i,
      );
      const [latestRes, earliestRes] = await Promise.all([
        admin.listOffsets({
          topics: [
            {
              name: route.dlqTopic,
              partitions: partitionIndices.map((p) => ({
                partitionIndex: p,
                timestamp: LATEST_OFFSET,
              })),
            },
          ],
        }),
        admin.listOffsets({
          topics: [
            {
              name: route.dlqTopic,
              partitions: partitionIndices.map((p) => ({
                partitionIndex: p,
                timestamp: EARLIEST_OFFSET,
              })),
            },
          ],
        }),
      ]);
      const latest = extractPartitions(latestRes, route.dlqTopic);
      const earliest = extractPartitions(earliestRes, route.dlqTopic);
      const out: PartitionDepth[] = [];
      for (const p of partitionIndices) {
        const hi = latest.get(p);
        const lo = earliest.get(p);
        if (hi === undefined || lo === undefined) {
          out.push({
            partition: p,
            depth: null,
            observedAt,
            ageMs: 0,
            depthError: "partition_offset_missing",
          });
          continue;
        }
        out.push({
          partition: p,
          depth: Math.max(0, Number(hi - lo)),
          observedAt,
          ageMs: 0,
        });
      }
      cache.set(route.name, out);
    } catch (err) {
      // Spec Risk 3: Redpanda's Admin path may not implement listOffsets.
      // Preserve the last good depth values (don't blow them away on a
      // transient probe failure); for routes we've never seen, record a
      // single sentinel so /admin/dlq surfaces the failure rather than an
      // empty partitions array (which would look like a healthy empty DLQ).
      const prev = cache.get(route.name) ?? [];
      const cls = errorClass(err);
      log.debug(
        { route: route.name, dlqTopic: route.dlqTopic, err: cls },
        "dlq depth probe failed; serving stale cache + depthError",
      );
      if (prev.length === 0) {
        cache.set(route.name, [
          {
            partition: -1,
            depth: null,
            observedAt,
            ageMs: 0,
            depthError: cls,
          },
        ]);
        return;
      }
      cache.set(
        route.name,
        prev.map((p) => ({ ...p, depthError: cls })),
      );
    }
  }

  function snapshot(name: string): PartitionDepth[] {
    const list = cache.get(name) ?? [];
    const now = Date.now();
    return list.map((p) => ({ ...p, ageMs: now - p.observedAt }));
  }

  async function probeAll(): Promise<void> {
    await Promise.all(routes.map((r) => probeRoute(r)));
  }

  return {
    get(route) {
      return snapshot(route);
    },
    async refresh(route) {
      const r = routes.find((x) => x.name === route);
      if (r === undefined) return;
      await probeRoute(r);
    },
    start() {
      if (timer !== null || stopped) return;
      // First probe runs immediately so /admin/dlq has data before the first
      // interval tick. Errors are swallowed (logged inside probeRoute).
      void probeAll();
      timer = setInterval(() => {
        if (stopped) return;
        void probeAll();
      }, probeIntervalMs);
      if (typeof (timer as { unref?: () => void }).unref === "function") {
        (timer as { unref: () => void }).unref();
      }
    },
    stop() {
      stopped = true;
      if (timer !== null) {
        clearInterval(timer);
        timer = null;
      }
    },
  };
}

function extractPartitions(
  raw: ListedOffsetsResult,
  topicName: string,
): Map<number, bigint> {
  const out = new Map<number, bigint>();
  for (const t of raw) {
    if (t.name !== topicName) continue;
    for (const p of t.partitions) {
      out.set(p.partitionIndex, p.offset);
    }
  }
  return out;
}
