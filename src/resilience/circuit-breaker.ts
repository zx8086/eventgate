// src/resilience/circuit-breaker.ts
import { CircuitBreakerOpenError } from "./errors.ts";

export type CircuitState = "closed" | "open" | "half-open";

export type CircuitBreakerConfig = {
  failureThreshold: number;
  successThreshold: number;
  recoveryTimeoutMs: number;
};

export type CircuitBreakerSnapshot = {
  state: CircuitState;
  failures: number;
  nextAttemptAt: number | null;
};

export type IsTransportError = (err: unknown) => boolean;
// onOpen receives the transport error message that triggered the trip (if any).
// undefined when fired from forceOpen() or any path without a recorded error.
export type OnOpen = (lastError?: string) => void;
export type OnHalfOpen = () => void;
export type OnClosed = () => void;

export type CircuitBreakerCallbacks = {
  isTransportError?: IsTransportError;
  onOpen?: OnOpen;
  onHalfOpen?: OnHalfOpen;
  onClosed?: OnClosed;
};

export class CircuitBreaker {
  private state: CircuitState = "closed";
  private failures = 0;
  private successes = 0;
  private nextAttemptTime: number | null = null;
  private readonly isTransportError: IsTransportError;
  private readonly onOpen: OnOpen;
  private readonly onHalfOpen: OnHalfOpen;
  private readonly onClosed: OnClosed;
  private pendingLastError: string | undefined;

  // The FSM is name-agnostic. Callers that need a name for log lines or
  // metric labels (e.g. ProducerHandle uses "kafka-producer") own that string
  // themselves — keeps this class portable per circuit-breaker-guide §4.
  constructor(
    private readonly config: CircuitBreakerConfig,
    callbacks: CircuitBreakerCallbacks = {},
  ) {
    this.isTransportError = callbacks.isTransportError ?? (() => true);
    this.onOpen = callbacks.onOpen ?? (() => {});
    this.onHalfOpen = callbacks.onHalfOpen ?? (() => {});
    this.onClosed = callbacks.onClosed ?? (() => {});
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.state === "open") {
      if (this.nextAttemptTime !== null && Date.now() >= this.nextAttemptTime) {
        this.transitionToHalfOpen();
      } else {
        throw new CircuitBreakerOpenError(new Date(this.nextAttemptTime ?? Date.now()));
      }
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      if (this.isTransportError(error)) {
        const message = error instanceof Error ? error.message : String(error);
        this.onFailure(message);
      }
      throw error;
    }
  }

  getState(): CircuitState {
    return this.state;
  }

  getSnapshot(): CircuitBreakerSnapshot {
    return {
      state: this.state,
      failures: this.failures,
      nextAttemptAt: this.nextAttemptTime,
    };
  }

  // Manual open for maintenance windows. Fires the onOpen callback for
  // observability parity with organic trips, and leaves `failures` at 0 —
  // so `state: "open", failures: 0` in a snapshot is a legitimate combination
  // (manual open) and not a sign of a counter bug. `lastError` is undefined
  // because there is no transport error to attribute the trip to.
  forceOpen(): void {
    this.transitionToOpen();
  }

  reset(): void {
    this.state = "closed";
    this.failures = 0;
    this.successes = 0;
    this.nextAttemptTime = null;
    this.pendingLastError = undefined;
  }

  private onSuccess(): void {
    this.failures = 0;
    if (this.state === "half-open") {
      this.successes += 1;
      if (this.successes >= this.config.successThreshold) {
        this.transitionToClosed();
      }
    }
  }

  private onFailure(message: string): void {
    if (this.state === "open") return;
    this.failures += 1;
    if (this.state === "half-open" || this.failures >= this.config.failureThreshold) {
      this.pendingLastError = message;
      this.transitionToOpen();
    }
  }

  private transitionToOpen(): void {
    this.state = "open";
    this.successes = 0;
    this.nextAttemptTime = Date.now() + this.config.recoveryTimeoutMs;
    const lastError = this.pendingLastError;
    this.pendingLastError = undefined;
    this.onOpen(lastError);
  }

  private transitionToHalfOpen(): void {
    this.state = "half-open";
    this.successes = 0;
    this.onHalfOpen();
  }

  private transitionToClosed(): void {
    this.state = "closed";
    this.failures = 0;
    this.successes = 0;
    this.nextAttemptTime = null;
    this.onClosed();
  }
}
