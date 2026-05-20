// src/outbox/metrics.ts

export type DrainMetricsSnapshot = {
  publishedLast60s: number;
  lastPublishedAt: number | null;
  lastError: { topic: string; message: string; at: number } | null;
};

export type DrainMetrics = {
  recordPublished(topic: string): void;
  recordError(topic: string, message: string): void;
  snapshot(): DrainMetricsSnapshot;
};

export type DrainMetricsOptions = {
  windowMs?: number;
  now?: () => number;
};

export function createDrainMetrics(opts: DrainMetricsOptions = {}): DrainMetrics {
  const windowMs = opts.windowMs ?? 60_000;
  const now = opts.now ?? Date.now;

  // Timestamped ring buffer of publish times. Trimmed on read.
  const publishedAt: number[] = [];
  let lastPublishedAt: number | null = null;
  let lastError: DrainMetricsSnapshot["lastError"] = null;

  const trim = (cutoff: number): void => {
    // publishedAt is push-only with monotonic timestamps, so a leading slice
    // is enough - no need for binary search.
    let drop = 0;
    while (drop < publishedAt.length && publishedAt[drop]! < cutoff) drop += 1;
    if (drop > 0) publishedAt.splice(0, drop);
  };

  return {
    recordPublished(_topic) {
      const t = now();
      publishedAt.push(t);
      lastPublishedAt = t;
      // Bound the buffer's worst-case memory at sustained 10k/sec for 60s.
      if (publishedAt.length > 1_000_000) publishedAt.splice(0, publishedAt.length - 600_000);
    },
    recordError(topic, message) {
      lastError = { topic, message, at: now() };
    },
    snapshot(): DrainMetricsSnapshot {
      const cutoff = now() - windowMs;
      trim(cutoff);
      return {
        publishedLast60s: publishedAt.length,
        lastPublishedAt,
        lastError,
      };
    },
  };
}
