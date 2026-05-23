// src/replay/triage.ts
import type { ReplayConfig } from "../config/schemas.ts";
import { parseAttempt, readHeader } from "./headers.ts";
import type { DlqRecord, TriageDecision } from "./types.ts";

// Decision tree (spec §"Triage decision tree"):
//   1. attempt header >= maxAttempts        -> park (exceeded_attempts)
//   2. exception class in poisonErrors      -> park (poison_class)
//   3. exception class in transientErrors   -> replay
//   4. fallback                              -> cfg.default
//
// readHeader iterates the raw tuple array with Buffer.equals so duplicate
// header names survive and Map<Buffer> reference-equality is not a factor.
// parseAttempt is Number()-strict with negative clamp so a malformed attempt
// header coerces to 0 instead of bypassing the cap.
export function triage(rec: DlqRecord, cfg: ReplayConfig): TriageDecision {
  const attempt = parseAttempt(readHeader(rec, "x-eventgate-replay-attempt"));
  if (attempt >= cfg.maxAttempts) {
    const exceptionClass =
      readHeader(rec, "__connect.errors.exception.class.name") ?? null;
    return { kind: "park", reason: "exceeded_attempts", exceptionClass };
  }

  const exceptionClass =
    readHeader(rec, "__connect.errors.exception.class.name") ?? null;

  if (exceptionClass !== null && cfg.poisonErrors.includes(exceptionClass)) {
    return { kind: "park", reason: "poison_class", exceptionClass };
  }
  if (exceptionClass !== null && cfg.transientErrors.includes(exceptionClass)) {
    return { kind: "replay", exceptionClass };
  }

  if (cfg.default === "replay") {
    return { kind: "replay", exceptionClass };
  }
  return { kind: "park", reason: "default_park", exceptionClass };
}

// Exposed so the endpoint and the runner read the same attempt the triage
// did — single source of truth.
export function getCurrentAttempt(rec: DlqRecord): number {
  return parseAttempt(readHeader(rec, "x-eventgate-replay-attempt"));
}
