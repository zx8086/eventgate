// src/health/types.ts

export type DependencyName = "kafkaProducer" | "outboxDb" | "kafkaBroker" | "topics";

export type DependencyStatus = {
  ok: boolean;
  lastCheckedAt: number;
  lastError?: string;
  // Per-dependency extras (kept loose because each probe carries different metadata):
  connected?: boolean;       // kafkaProducer
  brokerProbeMs?: number;    // kafkaBroker
  missing?: string[];        // topics
};

export type HealthSnapshot = {
  status: "healthy" | "degraded" | "unhealthy";
  ok: boolean;
  checkedAt: number;
  dependencies: Partial<Record<DependencyName, DependencyStatus>>;
};

export type HealthRequiredness = {
  // Dependencies that demote 200 -> 503 on failure. Topics are NOT in here —
  // missing topics produce status="degraded" but still return 200, because the
  // outbox is designed to ride out downstream gaps.
  required: ReadonlyArray<DependencyName>;
};

export const DEFAULT_REQUIREDNESS: HealthRequiredness = {
  required: ["kafkaProducer", "outboxDb", "kafkaBroker"],
};
