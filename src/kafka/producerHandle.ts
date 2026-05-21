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

// Single-line helper to keep the log shape consistent across the three
// transition events. Per circuit-breaker-guide §10, every transition log
// carries event_name, breaker, from, failures, and (when relevant) the
// next_attempt_at ISO timestamp.
function logTransition(
  log: ILogger,
  eventName:
    | "circuit_breaker_opened"
    | "circuit_breaker_closed"
    | "circuit_breaker_half_open",
  from: string,
  snapshot: CircuitBreakerSnapshot,
  message: string,
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
  let lastSeenState: CircuitBreakerSnapshot["state"] = "closed";

  const breaker = new CircuitBreaker(
    breakerConfig,
    (err) => !isApplicationLevelError(err),
    () => {
      // Increment caller-supplied counter first so a thrown logger never
      // breaks the metrics path.
      try {
        onBreakerOpen();
      } catch (cbErr) {
        log.warn(
          { err: cbErr instanceof Error ? cbErr.message : String(cbErr) },
          "breaker onOpen callback threw",
        );
      }
      logTransition(log, "circuit_breaker_opened", lastSeenState, breaker.getSnapshot(), "circuit breaker opened");
      lastSeenState = "open";
    },
  );

  const trackPostCallStateTransition = (): void => {
    const currentState = breaker.getSnapshot().state;
    if (lastSeenState !== currentState) {
      if (currentState === "closed") {
        logTransition(log, "circuit_breaker_closed", lastSeenState, breaker.getSnapshot(), "circuit breaker closed");
      } else if (currentState === "half-open") {
        logTransition(log, "circuit_breaker_half_open", lastSeenState, breaker.getSnapshot(), "circuit breaker half-open");
      }
      // open transitions are logged by the onOpen callback, not here, to
      // avoid double-logging.
      lastSeenState = currentState;
    }
  };

  return {
    isConnected: () => inner.isConnected(),
    disconnect: async () => {
      await inner.disconnect();
    },
    sendByTopic: async (topic, key, value, headers) => {
      try {
        await breaker.execute(() => inner.sendByTopic(topic, key, value, headers));
        trackPostCallStateTransition();
      } catch (err) {
        trackPostCallStateTransition();
        throw err;
      }
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
