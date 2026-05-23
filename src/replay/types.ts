// src/replay/types.ts

// Raw header tuple form preserved end-to-end so duplicate header names (which
// Kafka permits and the SDK's Map<Buffer,Buffer> collapses) survive replay.
export type HeaderTuple = [Buffer | null, Buffer | null];

export type DlqRecord = {
  topic: string;
  partition: number;
  offset: number;
  key: Buffer | null;
  value: Buffer | null;
  headers: HeaderTuple[];
  timestamp: number;
};

export type TriageDecision =
  | { kind: "replay"; exceptionClass: string | null }
  | {
      kind: "park";
      reason: "exceeded_attempts" | "poison_class" | "default_park";
      exceptionClass: string | null;
    };

export type ReplayJobMode = "manual" | "auto" | "single";
export type ReplayJobStatus =
  | "pending"
  | "running"
  | "paused"
  | "done"
  | "cancelled"
  | "failed";

export type ReplayJob = {
  id: string;
  route: string;
  partition: number;
  mode: ReplayJobMode;
  dryRun: boolean;
  scanned: number;
  replayed: number;
  parked: number;
  skipped: number;
  errors: number;
  fromOffset: number | null;
  toOffset: number | null;
  lastOffset: number | null;
  status: ReplayJobStatus;
  lastError: string | null;
  nextResumeAt: number | null;
  startedAt: number;
  finishedAt: number | null;
};

export type ReplayBatchResult = {
  scanned: number;
  replayed: number;
  parked: number;
  skipped: number;
  errors: number;
  lastOffset: number | null;
};

export type AuditHeaderInput = {
  jobId: string;
  sourceTopic: string;
  partition: number;
  offset: number;
  attempt: number;
};
