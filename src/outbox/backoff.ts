// src/outbox/backoff.ts

const BASE_MS = 1_000;

export function nextDelayMs(attempts: number, capMs: number): number {
  const n = attempts < 1 ? 1 : attempts;
  const exp = BASE_MS * 2 ** (n - 1);
  return Math.min(exp, capMs);
}
