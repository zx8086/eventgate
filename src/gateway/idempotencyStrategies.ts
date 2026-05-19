// src/gateway/idempotencyStrategies.ts
import { autoOpsIdempotencyKey } from "./idempotencyKey.ts";

export type IdempotencyStrategy = (body: unknown) => string | undefined;

export const idempotencyStrategies: Record<string, IdempotencyStrategy> = {
  "elastic-autoops": autoOpsIdempotencyKey,
};

export function knownIdempotencyStrategy(name: string): boolean {
  return Object.prototype.hasOwnProperty.call(idempotencyStrategies, name);
}

export function resolveIdempotencyStrategy(
  name: string | undefined,
): IdempotencyStrategy | undefined {
  if (name === undefined) return undefined;
  return idempotencyStrategies[name];
}
