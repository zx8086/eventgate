// src/replay/runner.ts
import type { EventProducer } from "../kafka/producer.ts";
import type { DlqRecord, ReplayBatchResult } from "./types.ts";

export type DryRunOpts = {
  records: AsyncIterable<DlqRecord>;
  // Accepted for signature parity with the future real runner; dry-run never
  // calls it. Kept in the type so callers can wire the same deps shape now
  // and have Phase 2/4 fill in the producer call without an interface change.
  producer: EventProducer;
};

// Phase 1 dry-run path: counts and tracks the last offset scanned. Triage
// classification ships in Phase 2; real produce/park calls ship in Phase 4.
// The dry-run output gives operators "how many records would I scan" before
// committing to a real replay.
export async function runReplayBatchDryRun(opts: DryRunOpts): Promise<ReplayBatchResult> {
  let scanned = 0;
  let lastOffset: number | null = null;
  for await (const rec of opts.records) {
    scanned += 1;
    lastOffset = rec.offset;
  }
  return {
    scanned,
    replayed: 0,
    parked: 0,
    skipped: 0,
    errors: 0,
    lastOffset,
  };
}
