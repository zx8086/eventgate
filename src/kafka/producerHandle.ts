// src/kafka/producerHandle.ts
import { getLogger, type ILogger } from "../logging/index.ts";
import {
  CircuitBreaker,
  type CircuitBreakerConfig,
  type CircuitBreakerSnapshot,
} from "../resilience/circuit-breaker.ts";
import { isApplicationLevelError } from "../resilience/errors.ts";
import type { KafkaProvider } from "./providers/index.ts";
import { createProducer, type EventProducer } from "./producer.ts";

export type ProducerHandle = EventProducer & {
  getBreakerSnapshot(): CircuitBreakerSnapshot;
};

const BREAKER_NAME = "kafka-producer";

// Per circuit-breaker-guide §10, every transition log carries event_name,
// breaker, from, failures, and (when relevant) the next_attempt_at ISO
// timestamp. The `last_error` field appears only on circuit_breaker_opened
// events, carrying the transport-error message that triggered the trip.
function logTransition(
  log: ILogger,
  eventName:
    | "circuit_breaker_opened"
    | "circuit_breaker_closed"
    | "circuit_breaker_half_open",
  from: string,
  snapshot: CircuitBreakerSnapshot,
  message: string,
  lastError?: string,
): void {
  const bindings: Record<string, unknown> = {
    event_name: eventName,
    breaker: BREAKER_NAME,
    from,
    failures: snapshot.failures,
  };
  if (snapshot.nextAttemptAt !== null) {
    bindings.next_attempt_at = new Date(snapshot.nextAttemptAt).toISOString();
  }
  if (lastError !== undefined) {
    bindings.last_error = lastError;
  }
  log.info(bindings, message);
}

// Internal factory — takes an already-constructed EventProducer. Used by
// the public createProducerHandle() factory AND by unit tests so they can
// inject a fake inner producer without going through createProducer().
export function createProducerHandleFromInner(
  inner: EventProducer,
  breakerConfig: CircuitBreakerConfig,
  onBreakerOpen: () => void = () => {},
  logger?: ILogger,
): ProducerHandle {
  const log = logger ?? getLogger("kafka.breaker");
  // State tracked separately from the FSM solely so transition logs can
  // include an accurate `from` field. The FSM drives every update via the
  // onOpen / onHalfOpen / onClosed callbacks, so this stays in sync.
  let lastSeenState: CircuitBreakerSnapshot["state"] = "closed";
  let breaker: CircuitBreaker;

  breaker = new CircuitBreaker(breakerConfig, {
    isTransportError: (err) => !isApplicationLevelError(err),
    onOpen: (lastError) => {
      // Increment caller-supplied counter first so a thrown counter never
      // breaks the log path.
      try {
        onBreakerOpen();
      } catch (cbErr) {
        log.warn(
          { err: cbErr instanceof Error ? cbErr.message : String(cbErr) },
          "breaker onOpen callback threw",
        );
      }
      logTransition(
        log,
        "circuit_breaker_opened",
        lastSeenState,
        breaker.getSnapshot(),
        "circuit breaker opened",
        lastError,
      );
      lastSeenState = "open";
    },
    onHalfOpen: () => {
      logTransition(
        log,
        "circuit_breaker_half_open",
        lastSeenState,
        breaker.getSnapshot(),
        "circuit breaker half-open",
      );
      lastSeenState = "half-open";
    },
    onClosed: () => {
      logTransition(
        log,
        "circuit_breaker_closed",
        lastSeenState,
        breaker.getSnapshot(),
        "circuit breaker closed",
      );
      lastSeenState = "closed";
    },
  });

  return {
    isConnected: () => inner.isConnected(),
    disconnect: async () => {
      await inner.disconnect();
    },
    sendByTopic: async (topic, key, value, headers) => {
      await breaker.execute(() => inner.sendByTopic(topic, key, value, headers));
    },
    getBreakerSnapshot: () => breaker.getSnapshot(),
  };
}

// Public factory — used by gateway/index.ts. Constructs a real Producer
// via createProducer(), then wraps it.
export async function createProducerHandle(
  clientId: string,
  provider: KafkaProvider,
  breakerConfig: CircuitBreakerConfig,
  onBreakerOpen: () => void = () => {},
): Promise<ProducerHandle> {
  const inner = await createProducer(clientId, provider);
  return createProducerHandleFromInner(inner, breakerConfig, onBreakerOpen);
}
