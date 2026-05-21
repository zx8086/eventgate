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
export type OnOpen = () => void;

export class CircuitBreaker {
  private state: CircuitState = "closed";
  private failures = 0;
  private successes = 0;
  private nextAttemptTime: number | null = null;

  // `name` is accepted positionally and reserved for downstream callers
  // (e.g. log lines in ProducerHandle). The FSM itself is name-agnostic, so
  // it's not stored as a field — keeps the class generic per
  // circuit-breaker-guide §4.
  constructor(
    _name: string,
    private readonly config: CircuitBreakerConfig,
    private readonly isTransportError: IsTransportError = () => true,
    private readonly onOpen: OnOpen = () => {},
  ) {}

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
        this.onFailure();
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

  forceOpen(): void {
    this.transitionToOpen();
  }

  reset(): void {
    this.state = "closed";
    this.failures = 0;
    this.successes = 0;
    this.nextAttemptTime = null;
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

  private onFailure(): void {
    if (this.state === "open") return;
    this.failures += 1;
    if (this.state === "half-open" || this.failures >= this.config.failureThreshold) {
      this.transitionToOpen();
    }
  }

  private transitionToOpen(): void {
    this.state = "open";
    this.successes = 0;
    this.nextAttemptTime = Date.now() + this.config.recoveryTimeoutMs;
    this.onOpen();
  }

  private transitionToHalfOpen(): void {
    this.state = "half-open";
    this.successes = 0;
  }

  private transitionToClosed(): void {
    this.state = "closed";
    this.failures = 0;
    this.successes = 0;
    this.nextAttemptTime = null;
  }
}
