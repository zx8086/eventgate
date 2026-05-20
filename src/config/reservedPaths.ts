// src/config/reservedPaths.ts
// Operational endpoints that config routes must not shadow.
// See: docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md

type Check = { ok: true } | { ok: false; message: string };

export const RESERVED_PATHS: ReadonlySet<string> = new Set([
  "/healthz",
  "/admin/routes",
]);

export function isReservedPath(path: string): boolean {
  return RESERVED_PATHS.has(path);
}

export function checkReservedPath(path: string): Check {
  if (RESERVED_PATHS.has(path)) {
    return {
      ok: false,
      message: `path '${path}' is reserved for operational use`,
    };
  }
  return { ok: true };
}
