// src/replay/headers.ts
import type { AuditHeaderInput, HeaderTuple } from "./types.ts";

const CONNECT_PREFIX = "__connect.errors.";

// readHeader iterates with Buffer.equals because Map<Buffer,Buffer>.get
// against a fresh Buffer.from() key returns undefined (reference equality).
// Verified by scripts/replay-sdk-smoke.ts; see docs/architecture/dlq-replay.md.
// Returns "" for present-but-null values; undefined for missing.
export function readHeader(
  rec: { headers: ReadonlyArray<HeaderTuple> },
  name: string,
): string | undefined {
  const needle = Buffer.from(name);
  for (const [k, v] of rec.headers) {
    if (k !== null && k.equals(needle)) {
      return v === null ? "" : v.toString("utf-8");
    }
  }
  return undefined;
}

export function stripConnectHeaders(
  headers: ReadonlyArray<HeaderTuple>,
): HeaderTuple[] {
  const out: HeaderTuple[] = [];
  for (const tuple of headers) {
    const [k, v] = tuple;
    if (k === null) {
      out.push([k, v]);
      continue;
    }
    if (k.toString("utf-8").startsWith(CONNECT_PREFIX)) continue;
    out.push([k, v]);
  }
  return out;
}

export function stampAuditHeaders(
  headers: ReadonlyArray<HeaderTuple>,
  audit: AuditHeaderInput,
): HeaderTuple[] {
  const append: HeaderTuple[] = [
    [Buffer.from("x-eventgate-replay-attempt"), Buffer.from(String(audit.attempt))],
    [Buffer.from("x-eventgate-replay-job-id"), Buffer.from(audit.jobId)],
    [Buffer.from("x-eventgate-replay-at"), Buffer.from(new Date().toISOString())],
    [Buffer.from("x-eventgate-replay-source-topic"), Buffer.from(audit.sourceTopic)],
    [
      Buffer.from("x-eventgate-replay-source-offset"),
      Buffer.from(`${audit.partition}:${audit.offset}`),
    ],
  ];
  return [...headers, ...append];
}

// Number() is strict — "5abc" -> NaN, unlike parseInt's permissive partial parse.
// Negative values clamp to 0 so a malformed header can never bypass the loop cap.
export function parseAttempt(raw: string | undefined): number {
  if (raw === undefined || raw === "") return 0;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.trunc(parsed));
}
