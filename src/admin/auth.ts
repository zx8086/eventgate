// src/admin/auth.ts
// Single-secret admin-endpoint authentication.
// See: docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md

import { timingSafeEqual } from "node:crypto";

export function verifyAdminToken(
  provided: string | undefined | null,
  expected: string,
): boolean {
  if (provided === undefined || provided === null || provided.length === 0) {
    return false;
  }
  const providedBuf = Buffer.from(provided);
  const expectedBuf = Buffer.from(expected);
  if (providedBuf.length !== expectedBuf.length) {
    return false;
  }
  return timingSafeEqual(providedBuf, expectedBuf);
}
