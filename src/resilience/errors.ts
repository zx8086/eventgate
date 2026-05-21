// src/resilience/errors.ts

export class CircuitBreakerOpenError extends Error {
  constructor(public readonly nextAttemptAt: Date) {
    super(`Circuit breaker OPEN - next attempt at ${nextAttemptAt.toISOString()}`);
    this.name = "CircuitBreakerOpenError";
  }
}

// Returns true ONLY for errors that are application-level (a normal broker
// response to a misrouted message, e.g. UNKNOWN_TOPIC_OR_PARTITION). All
// other errors are treated as transport failures and count toward the
// breaker's failure threshold.
//
// PR1 ships with the conservative default: every error counts. As we observe
// specific application-level patterns from @platformatic/kafka in production,
// we extend this predicate to return true for those — keeping the breaker
// unaffected by errors that aren't transport failures.
export function isApplicationLevelError(_err: unknown): boolean {
  return false;
}
