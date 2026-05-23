// src/config/reservedPaths.ts
// Operational endpoints that config routes must not shadow.
// See: docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md

type Check = { ok: true } | { ok: false; message: string };

export const RESERVED_PATHS: ReadonlySet<string> = new Set([
  "/healthz",
  "/admin/routes",
  "/admin/dlq",
  "/admin/replay",
]);

// Prefixes whose entire subtree is reserved. The replay subsystem registers
// parameterised paths like /admin/replay/:route and /admin/replay/:jobId/cancel;
// blocking the prefix prevents config routes from claiming any sub-path that
// would shadow them.
const RESERVED_PREFIXES: ReadonlyArray<string> = ["/admin/dlq/", "/admin/replay/"];

export function isReservedPath(path: string): boolean {
  if (RESERVED_PATHS.has(path)) return true;
  return RESERVED_PREFIXES.some((p) => path.startsWith(p));
}

export function checkReservedPath(path: string): Check {
  if (isReservedPath(path)) {
    return {
      ok: false,
      message: `path '${path}' is reserved for operational use`,
    };
  }
  return { ok: true };
}
