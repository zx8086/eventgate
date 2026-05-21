// src/health/types.ts

export type DependencyName = "kafkaProducer" | "outboxDb" | "kafkaBroker" | "topics";

export type DependencyStatus = {
  ok: boolean;
  lastCheckedAt: number;
  lastError?: string;
  // Per-dependency extras (kept loose because each probe carries different metadata):
  connected?: boolean;             // kafkaProducer
  breakerState?: "closed" | "open" | "half-open";  // kafkaProducer (SIO-817)
  breakerNextAttemptAt?: string;   // kafkaProducer (ISO timestamp, only when state === "open")
  brokerProbeMs?: number;          // kafkaBroker
  missing?: string[];              // topics
};

export type HealthSnapshot = {
  status: "healthy" | "degraded" | "unhealthy";
  ok: boolean;
  checkedAt: number;
  dependencies: Partial<Record<DependencyName, DependencyStatus>>;
};

export type HealthRequiredness = {
  // Dependencies that demote 200 -> 503 on failure.
  //
  // `kafkaBroker` and `topics` are NOT here — both produce status="degraded"
  // with HTTP 200 when their probe fails. Rationale: (a) the outbox is
  // designed to ride out downstream gaps so missing topics shouldn't take the
  // gateway out of rotation; (b) the Admin API probe is not portable across
  // every broker we deploy against (e.g. some Redpanda admin paths are not
  // implemented by @platformatic/kafka's Admin client), so a probe failure
  // doesn't actually mean the producer can't publish. The producer's own
  // `isConnected()` flag remains the load-bearing signal for "can we accept
  // webhook writes."
  required: ReadonlyArray<DependencyName>;
};

export const DEFAULT_REQUIREDNESS: HealthRequiredness = {
  required: ["kafkaProducer", "outboxDb"],
};
