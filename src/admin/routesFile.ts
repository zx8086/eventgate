// src/admin/routesFile.ts
// Atomic-write persistence for the admin-mutable routes array.
// See: docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md

import { renameSync } from "node:fs";

export async function readRoutesFile(path: string): Promise<unknown[]> {
  const file = Bun.file(path);
  // Bun.file().json() throws ENOENT on missing files and SyntaxError on bad JSON.
  const parsed = await file.json();
  if (!Array.isArray(parsed)) {
    throw new Error(`expected JSON array at ${path}; got ${typeof parsed}`);
  }
  return parsed;
}

export async function writeRoutesFile(
  path: string,
  routes: unknown[],
): Promise<void> {
  // Atomic: write to <path>.tmp, then rename to <path>. POSIX rename is
  // atomic on same filesystem; cross-fs falls back to copy+unlink.
  const tmp = `${path}.tmp`;
  await Bun.write(tmp, JSON.stringify(routes, null, 2));
  renameSync(tmp, path);
}
