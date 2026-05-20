# SIO-803 Implementation Plan: Reserved paths + admin routes endpoint

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reserved-path guard so config routes cannot shadow `/healthz` or `/admin/routes`, and add a `PUT /admin/routes` admin endpoint (token-auth, Zod-validated, full-replacement, hot-reloaded via `server.reload`, persisted to a mounted `ROUTES_FILE`).

**Architecture:** Reserved-path validation is a small Zod cross-field check in the existing `routesSchema.superRefine`, modelled exactly on the topic-naming policy pattern. The admin endpoint lives in a new `src/admin/` directory (3 files: `auth.ts`, `routesFile.ts`, `routesEndpoint.ts`), wired into `src/gateway/index.ts` conditionally on `config.admin?.token`. The `buildConfig` loader gains a `ROUTES_FILE` source that takes precedence over `ROUTES_JSON`. Existing tests + the lazy Proxy + `resetConfigCache()` pattern are reused.

**Tech Stack:** Bun 1.3 (`Bun.serve.reload()`, `Bun.file`, `Bun.write`), TypeScript strict, Zod v4 (`strictObject`, `superRefine`), Pino 10 + ECS, `bun:test`. New: `node:crypto.timingSafeEqual`, `node:fs.renameSync`. No external dependencies.

**Reference spec:** `docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md`. Read it before starting — it contains the rationale and the trade-off table.

---

## File Structure

**New files**
- `src/config/reservedPaths.ts` — `RESERVED_PATHS` set, `checkReservedPath(path): Check`.
- `src/admin/auth.ts` — `verifyAdminToken(provided, expected): boolean` via `crypto.timingSafeEqual`.
- `src/admin/routesFile.ts` — `readRoutesFile(path): unknown[]`, `writeRoutesFile(path, routes): Promise<void>` with atomic-write.
- `src/admin/routesEndpoint.ts` — `makeAdminRoutesHandler(opts)` returning the PUT handler.
- `test/unit/config.reservedPaths.test.ts`
- `test/unit/config.routes.reserved.test.ts`
- `test/unit/admin.auth.test.ts`
- `test/unit/admin.routesFile.test.ts`
- `test/unit/admin.routesEndpoint.test.ts`
- `test/unit/config.loader.routesFile.test.ts`

**Modified files**
- `src/config/schemas.ts` — add `admin: z.strictObject({ token }).optional()` + `routesFile: z.string().min(1).optional()` to `configSchema`; insert reserved-path check in `routesSchema.superRefine`.
- `src/config/defaults.ts` — leave new optional fields unset (they're `optional()`).
- `src/config/envMapping.ts` — map `ADMIN_TOKEN` → `admin.token`; `ROUTES_FILE` → `routesFile`.
- `src/config/loader.ts` — file > env > defaults precedence for routes.
- `src/gateway/index.ts` — conditionally register `/admin/routes` when `config.admin?.token` set; capture `server` reference; rebuild on reload.
- `CLAUDE.md` — append the spec/plan linkage rule + document admin endpoint, `ADMIN_TOKEN`, `ROUTES_FILE`, `RESERVED_PATHS`.

---

## Task 1: Reserved-paths helper

**Why first:** Standalone helper with no dependencies; later tasks plug it into Zod + the admin endpoint. Identical shape to `src/config/topicPolicy.ts` from SIO-802.

**Files:**
- Create: `src/config/reservedPaths.ts`
- Test: `test/unit/config.reservedPaths.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.reservedPaths.test.ts
import { describe, expect, it } from "bun:test";
import { RESERVED_PATHS, checkReservedPath, isReservedPath } from "../../src/config/reservedPaths.ts";

describe("RESERVED_PATHS", () => {
  it("contains /healthz", () => {
    expect(RESERVED_PATHS.has("/healthz")).toBe(true);
  });
  it("contains /admin/routes", () => {
    expect(RESERVED_PATHS.has("/admin/routes")).toBe(true);
  });
});

describe("isReservedPath", () => {
  it("returns true for reserved paths", () => {
    expect(isReservedPath("/healthz")).toBe(true);
    expect(isReservedPath("/admin/routes")).toBe(true);
  });
  it("returns false for non-reserved paths", () => {
    expect(isReservedPath("/webhooks/elastic/autoops")).toBe(false);
    expect(isReservedPath("/admin")).toBe(false);
    expect(isReservedPath("/admin/routes/x")).toBe(false);
  });
});

describe("checkReservedPath", () => {
  it("returns ok for a non-reserved path", () => {
    expect(checkReservedPath("/webhooks/datadog")).toEqual({ ok: true });
  });
  it("returns a distinct message for /healthz", () => {
    const r = checkReservedPath("/healthz");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/'\/healthz' is reserved/);
  });
  it("returns a distinct message for /admin/routes", () => {
    const r = checkReservedPath("/admin/routes");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/'\/admin\/routes' is reserved/);
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/config.reservedPaths.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

```ts
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
```

- [ ] **Step 4: Verify it passes**

Run: `bun test test/unit/config.reservedPaths.test.ts`
Expected: ALL PASS.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/config/reservedPaths.ts test/unit/config.reservedPaths.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: reserved operational paths helper

Adds RESERVED_PATHS set with /healthz and /admin/routes plus
checkReservedPath(path) returning the same { ok, message } discriminated
union as checkGatewayTopic. Mirrors the topic-policy pattern.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Wire reserved-path check into routesSchema

**Files:**
- Modify: `src/config/schemas.ts`
- Test: `test/unit/config.routes.reserved.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.routes.reserved.test.ts
import { describe, expect, it } from "bun:test";
import { routesSchema, type RouteConfig } from "../../src/config/schemas.ts";

const base: RouteConfig = {
  name: "x",
  path: "/webhooks/x",
  topic: "T_PRIVATE_SOURCE_X_Y",
  keyFields: ["id"],
};

describe("routesSchema rejects reserved paths", () => {
  it("rejects /healthz with a reserved-path message", () => {
    const r = routesSchema.safeParse([{ ...base, path: "/healthz" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /'\/healthz' is reserved/.test(i.message))).toBe(true);
    }
  });

  it("rejects /admin/routes with a reserved-path message", () => {
    const r = routesSchema.safeParse([{ ...base, path: "/admin/routes" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /'\/admin\/routes' is reserved/.test(i.message))).toBe(true);
    }
  });

  it("accepts non-reserved paths", () => {
    expect(routesSchema.safeParse([base]).success).toBe(true);
  });

  it("accepts /admin (no trailing /routes)", () => {
    expect(routesSchema.safeParse([{ ...base, path: "/admin" }]).success).toBe(true);
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/config.routes.reserved.test.ts`
Expected: FAIL (Zod accepts the reserved path today).

- [ ] **Step 3: Insert the check into `routesSchema.superRefine`**

Open `src/config/schemas.ts`. Add to the import block at top:

```ts
import { checkReservedPath } from "./reservedPaths.ts";
```

Inside the existing `routesSchema.superRefine((routes, ctx) => { ... routes.forEach((r, i) => { ... }) })` block, immediately after the `topicCheck` block (right after the closing `}` of the `if (!topicCheck.ok)` block, before the `if (r.dlqTopic !== undefined)` block), add:

```ts
      const reservedCheck = checkReservedPath(r.path);
      if (!reservedCheck.ok) {
        ctx.addIssue({
          code: "custom",
          path: [i, "path"],
          message: reservedCheck.message,
        });
      }
```

- [ ] **Step 4: Verify it passes**

Run: `bun test test/unit/config.routes.reserved.test.ts`
Expected: PASS.

- [ ] **Step 5: Full suite + typecheck**

Run: `bun test && bun run typecheck`
Expected: ALL PASS; clean.

- [ ] **Step 6: Commit**

```bash
git add src/config/schemas.ts test/unit/config.routes.reserved.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: routesSchema rejects routes shadowing operational paths

Any route declaring path /healthz or /admin/routes fails startup with
the reserved-path message. Uses checkReservedPath from the helper added
in the previous commit; same superRefine ctx.addIssue pattern as the
topic-policy and duplicate-path checks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Admin token auth helper

**Files:**
- Create: `src/admin/auth.ts`
- Test: `test/unit/admin.auth.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/admin.auth.test.ts
import { describe, expect, it } from "bun:test";
import { verifyAdminToken } from "../../src/admin/auth.ts";

const TOKEN = "a".repeat(32);

describe("verifyAdminToken", () => {
  it("returns true for an exact match", () => {
    expect(verifyAdminToken(TOKEN, TOKEN)).toBe(true);
  });

  it("returns false for a mismatched token of the same length", () => {
    const wrong = "b".repeat(32);
    expect(verifyAdminToken(wrong, TOKEN)).toBe(false);
  });

  it("returns false for a token of different length", () => {
    expect(verifyAdminToken("short", TOKEN)).toBe(false);
    expect(verifyAdminToken(TOKEN + "x", TOKEN)).toBe(false);
  });

  it("returns false for undefined/empty provided", () => {
    expect(verifyAdminToken(undefined, TOKEN)).toBe(false);
    expect(verifyAdminToken("", TOKEN)).toBe(false);
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/admin.auth.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

```ts
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
```

- [ ] **Step 4: Verify it passes**

Run: `bun test test/unit/admin.auth.test.ts`
Expected: PASS.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/admin/auth.ts test/unit/admin.auth.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: admin-endpoint token verification (timing-safe)

Single-secret check using crypto.timingSafeEqual; length mismatch short-
circuits before the comparison (different-length buffers throw). Reusable
by any future admin endpoint.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Routes-file read/write helper

**Files:**
- Create: `src/admin/routesFile.ts`
- Test: `test/unit/admin.routesFile.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/admin.routesFile.test.ts
import { afterEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readRoutesFile, writeRoutesFile } from "../../src/admin/routesFile.ts";

let dir: string;

afterEach(() => {
  if (dir) rmSync(dir, { recursive: true, force: true });
});

function mkdir(): string {
  dir = mkdtempSync(join(tmpdir(), "eventgate-routes-"));
  return dir;
}

describe("readRoutesFile", () => {
  it("returns the parsed array", async () => {
    const d = mkdir();
    const path = join(d, "routes.json");
    writeFileSync(path, JSON.stringify([{ name: "a" }]));
    const result = await readRoutesFile(path);
    expect(result).toEqual([{ name: "a" }]);
  });

  it("throws when the file does not exist", async () => {
    const d = mkdir();
    const path = join(d, "missing.json");
    await expect(readRoutesFile(path)).rejects.toThrow();
  });

  it("throws when the file is not valid JSON", async () => {
    const d = mkdir();
    const path = join(d, "bad.json");
    writeFileSync(path, "not json");
    await expect(readRoutesFile(path)).rejects.toThrow();
  });

  it("throws when the parsed value is not an array", async () => {
    const d = mkdir();
    const path = join(d, "obj.json");
    writeFileSync(path, JSON.stringify({ foo: "bar" }));
    await expect(readRoutesFile(path)).rejects.toThrow(/expected JSON array/i);
  });
});

describe("writeRoutesFile", () => {
  it("writes the routes atomically (no .tmp left behind on success)", async () => {
    const d = mkdir();
    const path = join(d, "routes.json");
    await writeRoutesFile(path, [{ name: "a", path: "/x" }]);
    const round = await readRoutesFile(path);
    expect(round).toEqual([{ name: "a", path: "/x" }]);
    expect(existsSync(`${path}.tmp`)).toBe(false);
  });

  it("overwrites an existing file", async () => {
    const d = mkdir();
    const path = join(d, "routes.json");
    await writeRoutesFile(path, [{ name: "old" }]);
    await writeRoutesFile(path, [{ name: "new" }]);
    expect(await readRoutesFile(path)).toEqual([{ name: "new" }]);
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/admin.routesFile.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

```ts
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
```

- [ ] **Step 4: Verify it passes**

Run: `bun test test/unit/admin.routesFile.test.ts`
Expected: PASS.

- [ ] **Step 5: Typecheck + full suite**

Run: `bun test && bun run typecheck`
Expected: ALL PASS; clean.

- [ ] **Step 6: Commit**

```bash
git add src/admin/routesFile.ts test/unit/admin.routesFile.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: atomic-write helpers for the admin-mutable routes file

readRoutesFile uses Bun.file().json() and rejects non-array roots.
writeRoutesFile writes to <path>.tmp then renameSync — POSIX-atomic
on same filesystem.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Admin + routesFile config schema fields

**Files:**
- Modify: `src/config/schemas.ts`, `src/config/envMapping.ts`
- Test: `test/unit/config.admin.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.admin.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig } from "../../src/config/loader.ts";

let snapshot: NodeJS.ProcessEnv;
const baseEnv = { ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };

beforeEach(() => { snapshot = { ...process.env }; });
afterEach(() => { process.env = snapshot; });

describe("config.admin", () => {
  it("is undefined when ADMIN_TOKEN is unset", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.admin).toBeUndefined();
  });

  it("populates admin.token when ADMIN_TOKEN is set", () => {
    const token = "a".repeat(32);
    const cfg = buildConfig({ ...baseEnv, ADMIN_TOKEN: token });
    expect(cfg.admin?.token).toBe(token);
  });

  it("rejects ADMIN_TOKEN shorter than 32 characters", () => {
    expect(() => buildConfig({ ...baseEnv, ADMIN_TOKEN: "tooshort" })).toThrow(/Invalid configuration/);
  });
});

describe("config.routesFile", () => {
  it("is undefined when ROUTES_FILE is unset", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.routesFile).toBeUndefined();
  });

  it("is populated when ROUTES_FILE is set", () => {
    const cfg = buildConfig({ ...baseEnv, ROUTES_FILE: "/tmp/routes.json" });
    expect(cfg.routesFile).toBe("/tmp/routes.json");
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/config.admin.test.ts`
Expected: FAIL — `admin` and `routesFile` are unknown.

- [ ] **Step 3: Add schema fields**

In `src/config/schemas.ts`, inside the existing top-level `configSchema = z.strictObject({ ... })`, add these fields adjacent to `outbox`:

```ts
    admin: z.strictObject({
      token: z
        .string()
        .min(32, "ADMIN_TOKEN must be at least 32 characters")
        .describe("Shared secret protecting the /admin/routes endpoint."),
    })
      .optional()
      .describe("When present, enables the /admin/routes PUT endpoint."),
    routesFile: z
      .string()
      .min(1)
      .optional()
      .describe("Path to a mounted JSON file holding the routes array. When set, takes precedence over ROUTES_JSON."),
```

(Keep them adjacent to `outbox`, before `routes`. The exact order is up to the implementer; just keep them all inside the same `strictObject`.)

- [ ] **Step 4: Map env vars**

In `src/config/envMapping.ts`:

Add to the `EnvOverrides` type:
```ts
  admin?: { token?: string };
  routesFile?: string;
```

In `mapEnv`, after the existing nested sections and before the prune loop, add:

```ts
  const adminToken = str(env.ADMIN_TOKEN);
  if (adminToken !== undefined) {
    overrides.admin = { token: adminToken };
  }

  const routesFile = str(env.ROUTES_FILE);
  if (routesFile !== undefined) {
    overrides.routesFile = routesFile;
  }
```

These both flow through `mergeDeep` and `routes` is handled separately (Task 6 changes loader to read the file). Note: `admin.token` is a nested object, while `routesFile` is top-level — the prune loop's `!Array.isArray(section)` guard from SIO-802 already protects against any new edge cases.

- [ ] **Step 5: Verify it passes**

Run: `bun test test/unit/config.admin.test.ts`
Expected: ALL PASS.

- [ ] **Step 6: Full suite + typecheck**

Run: `bun test && bun run typecheck`
Expected: ALL PASS; clean.

- [ ] **Step 7: Commit**

```bash
git add src/config/schemas.ts src/config/envMapping.ts test/unit/config.admin.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: admin.token + routesFile config fields

Two new optional fields on AppConfig. ADMIN_TOKEN populates admin.token
(min 32 chars enforced by Zod); ROUTES_FILE populates routesFile. The
admin endpoint stays disabled unless ADMIN_TOKEN is set; the routes file
takes precedence over ROUTES_JSON (loader change in the next commit).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: ROUTES_FILE precedence in loader

**Files:**
- Modify: `src/config/loader.ts`
- Test: `test/unit/config.loader.routesFile.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.loader.routesFile.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildConfig } from "../../src/config/loader.ts";

let snapshot: NodeJS.ProcessEnv;
let dir: string;
const baseEnv = { ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };

beforeEach(() => {
  snapshot = { ...process.env };
  dir = mkdtempSync(join(tmpdir(), "eventgate-loader-"));
});
afterEach(() => {
  process.env = snapshot;
  if (dir) rmSync(dir, { recursive: true, force: true });
});

const fileRoute = [
  {
    name: "file-route",
    path: "/webhooks/file",
    topic: "T_PRIVATE_SOURCE_FILE_X",
    keyFields: ["id"],
  },
];

const envRoute = [
  {
    name: "env-route",
    path: "/webhooks/env",
    topic: "T_PRIVATE_SOURCE_ENV_X",
    keyFields: ["id"],
  },
];

describe("buildConfig routes source precedence", () => {
  it("uses ROUTES_FILE when set and present, ignoring ROUTES_JSON", () => {
    const path = join(dir, "routes.json");
    writeFileSync(path, JSON.stringify(fileRoute));
    const cfg = buildConfig({
      ...baseEnv,
      ROUTES_FILE: path,
      ROUTES_JSON: JSON.stringify(envRoute),
    });
    expect(cfg.routes[0]?.name).toBe("file-route");
  });

  it("falls through to ROUTES_JSON when ROUTES_FILE is unset", () => {
    const cfg = buildConfig({
      ...baseEnv,
      ROUTES_JSON: JSON.stringify(envRoute),
    });
    expect(cfg.routes[0]?.name).toBe("env-route");
  });

  it("falls through to defaults when neither is set", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.routes[0]?.name).toBe("elastic-autoops");
  });

  it("throws when ROUTES_FILE is set but the file does not exist", () => {
    expect(() => buildConfig({ ...baseEnv, ROUTES_FILE: join(dir, "missing.json") })).toThrow();
  });

  it("throws when ROUTES_FILE points at non-JSON", () => {
    const path = join(dir, "bad.json");
    writeFileSync(path, "not json");
    expect(() => buildConfig({ ...baseEnv, ROUTES_FILE: path })).toThrow();
  });

  it("throws when ROUTES_FILE points at a JSON object instead of array", () => {
    const path = join(dir, "obj.json");
    writeFileSync(path, JSON.stringify({ foo: "bar" }));
    expect(() => buildConfig({ ...baseEnv, ROUTES_FILE: path })).toThrow();
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/config.loader.routesFile.test.ts`
Expected: FAIL — loader doesn't consult `ROUTES_FILE` yet.

- [ ] **Step 3: Update `src/config/loader.ts`**

Add an import:
```ts
import { readFileSync } from "node:fs";
```

Note: we use sync read here because `buildConfig` is called from the lazy Proxy and must be synchronous. `Bun.file().json()` is async; for the loader we use `JSON.parse(readFileSync(path, "utf8"))` directly.

Replace `buildConfig`:

```ts
export function buildConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const overrides: EnvOverrides = mapEnv(env);

  // `routes` resolution: file > env > defaults. Pull `routes` and `routesFile`
  // out of overrides; merge the rest; then attach the resolved routes.
  const { routes: routesOverride, routesFile, ...rest } = overrides;
  const merged = mergeDeep(defaults as unknown as AppConfig, rest);

  let resolvedRoutes: unknown = (defaults as unknown as AppConfig).routes;
  if (routesFile !== undefined) {
    // File source — fail fast on read / parse / shape errors.
    const raw = readFileSync(routesFile, "utf8");
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error(`ROUTES_FILE ${routesFile} must contain a JSON array`);
    }
    resolvedRoutes = parsed;
  } else if (routesOverride !== undefined) {
    resolvedRoutes = routesOverride;
  }

  const withRoutes: AppConfig = {
    ...merged,
    routesFile,
    routes: resolvedRoutes as AppConfig["routes"],
  };

  const result = configSchema.safeParse(withRoutes);
  if (!result.success) {
    const summary = result.error.issues
      .map((i) => `  - ${i.path.join(".") || "<root>"}: ${i.message}`)
      .join("\n");
    throw new Error(`Invalid configuration:\n${summary}`);
  }
  return result.data;
}
```

Two notes for the implementer:
- The `readFileSync` errors (ENOENT, permission, etc.) bubble up — Zod-style "Invalid configuration" wrapping isn't required; the underlying error message is informative.
- `JSON.parse` errors bubble up the same way.

- [ ] **Step 4: Verify it passes**

Run: `bun test test/unit/config.loader.routesFile.test.ts`
Expected: ALL PASS.

- [ ] **Step 5: Full suite + typecheck**

Run: `bun test && bun run typecheck`
Expected: ALL PASS (existing `ROUTES_JSON` tests still pass — they don't set `ROUTES_FILE`); clean.

- [ ] **Step 6: Commit**

```bash
git add src/config/loader.ts test/unit/config.loader.routesFile.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: loader reads ROUTES_FILE when set; file > env > defaults

buildConfig now resolves routes from ROUTES_FILE first, falling back to
ROUTES_JSON, then defaults. Sync read (readFileSync + JSON.parse) so the
lazy config Proxy stays synchronous. ENOENT, bad JSON, and non-array
files all fail startup.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Admin PUT handler

**Files:**
- Create: `src/admin/routesEndpoint.ts`
- Test: `test/unit/admin.routesEndpoint.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/admin.routesEndpoint.test.ts
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeAdminRoutesHandler } from "../../src/admin/routesEndpoint.ts";

const TOKEN = "a".repeat(32);
let dir: string;
let routesFilePath: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "eventgate-admin-"));
  routesFilePath = join(dir, "routes.json");
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

const validRoutes = [
  {
    name: "datadog",
    path: "/webhooks/datadog",
    topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
    keyFields: ["alert_id"],
  },
];

function req(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request("http://localhost/admin/routes", {
    method: "PUT",
    headers: { "content-type": "application/json", ...headers },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

describe("makeAdminRoutesHandler", () => {
  it("rejects with 401 when X-Admin-Token is missing", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req(validRoutes));
    expect(res.status).toBe(401);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 401 when X-Admin-Token is wrong", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req(validRoutes, { "x-admin-token": "wrong" }));
    expect(res.status).toBe(401);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 400 when the body is not JSON", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req("not json", { "x-admin-token": TOKEN }));
    expect(res.status).toBe(400);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 400 when the body fails routesSchema (forbidden topic)", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const bad = [{ name: "x", path: "/x", topic: "T_PRIVATE_SINK_X_Y", keyFields: ["id"] }];
    const res = await handler(req(bad, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(400);
    const body = await res.json() as { issues: unknown[] };
    expect(body.issues.length).toBeGreaterThan(0);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 400 when the body declares a reserved path", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const bad = [{ name: "x", path: "/healthz", topic: "T_PRIVATE_SOURCE_X_Y", keyFields: ["id"] }];
    const res = await handler(req(bad, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(400);
    expect(reload).not.toHaveBeenCalled();
  });

  it("persists, reloads, and returns 200 on success", async () => {
    const reload = mock((routes: unknown[]) => { void routes; });
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req(validRoutes, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(200);
    // File persisted
    const fileContents = JSON.parse(readFileSync(routesFilePath, "utf8"));
    expect(fileContents[0].name).toBe("datadog");
    // Reload invoked with the validated routes
    expect(reload).toHaveBeenCalledTimes(1);
    const reloadArg = reload.mock.calls[0]?.[0] as { name: string }[];
    expect(reloadArg[0]?.name).toBe("datadog");
    // Response body echoes the routes
    const body = await res.json() as { routes: { name: string }[] };
    expect(body.routes[0]?.name).toBe("datadog");
  });

  it("returns 500 if persistence fails (read-only dir)", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath: "/nonexistent/dir/routes.json",
      onReload: reload,
    });
    const res = await handler(req(validRoutes, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(500);
    expect(reload).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/admin.routesEndpoint.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**

```ts
// src/admin/routesEndpoint.ts
// PUT /admin/routes handler: token auth, Zod validation, atomic-write
// persistence, hot reload. See: docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md

import { routesSchema, type RouteConfig } from "../config/schemas.ts";
import { getLogger } from "../logging/index.ts";
import { verifyAdminToken } from "./auth.ts";
import { writeRoutesFile } from "./routesFile.ts";

const log = getLogger("admin.routesEndpoint");

export type AdminRoutesDeps = {
  expectedToken: string;
  routesFilePath: string;
  onReload: (routes: RouteConfig[]) => void;
};

export function makeAdminRoutesHandler(deps: AdminRoutesDeps) {
  const { expectedToken, routesFilePath, onReload } = deps;

  return async function handler(req: Request): Promise<Response> {
    const provided = req.headers.get("x-admin-token");
    if (!verifyAdminToken(provided, expectedToken)) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }

    let body: unknown;
    try {
      body = await req.json();
    } catch {
      return Response.json({ error: "invalid JSON body" }, { status: 400 });
    }

    const parsed = routesSchema.safeParse(body);
    if (!parsed.success) {
      return Response.json(
        {
          error: "validation",
          issues: parsed.error.issues.map((i) => ({
            path: i.path,
            message: i.message,
          })),
        },
        { status: 400 },
      );
    }

    try {
      await writeRoutesFile(routesFilePath, parsed.data);
    } catch (err) {
      log.error({ err, routesFilePath }, "admin routes persist failed");
      return Response.json({ error: "persist failed" }, { status: 500 });
    }

    try {
      onReload(parsed.data);
    } catch (err) {
      log.error({ err }, "admin routes reload failed");
      return Response.json(
        {
          error: "reload failed",
          message: "routes persisted; restart will apply",
        },
        { status: 500 },
      );
    }

    return Response.json({ routes: parsed.data }, { status: 200 });
  };
}
```

- [ ] **Step 4: Verify it passes**

Run: `bun test test/unit/admin.routesEndpoint.test.ts`
Expected: ALL PASS.

- [ ] **Step 5: Full suite + typecheck**

Run: `bun test && bun run typecheck`
Expected: ALL PASS; clean.

- [ ] **Step 6: Commit**

```bash
git add src/admin/routesEndpoint.ts test/unit/admin.routesEndpoint.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: admin PUT /admin/routes handler

Token auth via verifyAdminToken; Zod validation via the same routesSchema
that gates startup; atomic-write persistence via writeRoutesFile; hot
reload via injected onReload callback. 401 on auth fail, 400 on body or
validation fail, 500 on persistence or reload fail. The handler is a pure
closure suitable for plugging into the Bun.serve routes map.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Wire admin endpoint into gateway/index.ts

**Files:**
- Modify: `src/gateway/index.ts`
- Test: `test/unit/gateway.index.adminEnabled.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/gateway.index.adminEnabled.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { buildRoutes } from "../../src/gateway/routes.ts";

const noopProducer = {
  publishRaw: async () => {},
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};
const noopOutbox = {
  enqueue: () => {},
  backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0 }),
};

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => { snapshot = { ...process.env }; resetConfigCache(); });
afterEach(() => { process.env = snapshot; resetConfigCache(); });

describe("buildRoutes admin endpoint registration", () => {
  it("does NOT register /admin/routes when ADMIN_TOKEN is unset", () => {
    process.env = { ...process.env, ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };
    delete process.env.ADMIN_TOKEN;
    resetConfigCache();
    const routes = buildRoutes({ producer: noopProducer, outbox: noopOutbox });
    expect(routes["/admin/routes"]).toBeUndefined();
  });

  it("registers /admin/routes PUT when ADMIN_TOKEN is set", () => {
    process.env = {
      ...process.env,
      ENVIRONMENT: "dev",
      KAFKA_PROVIDER: "local",
      ADMIN_TOKEN: "a".repeat(32),
    };
    resetConfigCache();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox: noopOutbox,
      adminContext: {
        onReload: () => {},
        routesFilePath: "/tmp/eventgate-test-routes.json",
      },
    });
    const adminEntry = routes["/admin/routes"];
    expect(adminEntry).toBeDefined();
    expect((adminEntry as { PUT?: unknown }).PUT).toBeDefined();
  });

  it("does NOT register /admin/routes when ADMIN_TOKEN set but adminContext is not passed", () => {
    process.env = {
      ...process.env,
      ENVIRONMENT: "dev",
      KAFKA_PROVIDER: "local",
      ADMIN_TOKEN: "a".repeat(32),
    };
    resetConfigCache();
    const routes = buildRoutes({ producer: noopProducer, outbox: noopOutbox });
    expect(routes["/admin/routes"]).toBeUndefined();
  });
});
```

This test requires `buildRoutes` to accept an optional `adminContext` so the admin handler can be injected from `gateway/index.ts` (which holds the `server` reference). Without `adminContext` the admin endpoint stays off even if `ADMIN_TOKEN` is set — this keeps the test seam clean and avoids `buildRoutes` reaching for global server state.

- [ ] **Step 2: Verify it fails**

Run: `bun test test/unit/gateway.index.adminEnabled.test.ts`
Expected: FAIL — `adminContext` is not a valid `buildRoutes` option.

- [ ] **Step 3: Update `src/gateway/routes.ts`**

Extend `RouteDeps` and the `buildRoutes` body. At the top of the file, add:

```ts
import { makeAdminRoutesHandler } from "../admin/routesEndpoint.ts";
import type { RouteConfig } from "../config/schemas.ts";
```

Update the exported `RouteDeps` type:

```ts
export type AdminContext = {
  routesFilePath: string;
  onReload: (routes: RouteConfig[]) => void;
};

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
  adminContext?: AdminContext;
};
```

Update the `RoutesMap` type to include a `PUT` shape:

```ts
type RoutesMap = Record<
  string,
  | (() => Response)
  | { POST: RouteHandler }
  | { PUT: RouteHandler }
>;
```

(Where `RouteHandler = (req: Request) => Promise<Response>` already exists in the file.)

Inside `buildRoutes`, after the webhook-route loop and before `return routes`, add:

```ts
  if (config.admin?.token && deps.adminContext) {
    routes["/admin/routes"] = {
      PUT: makeAdminRoutesHandler({
        expectedToken: config.admin.token,
        routesFilePath: deps.adminContext.routesFilePath,
        onReload: deps.adminContext.onReload,
      }),
    };
    log.info(
      { path: "/admin/routes", routesFilePath: deps.adminContext.routesFilePath },
      "admin endpoint enabled",
    );
  } else if (config.admin?.token) {
    log.warn(
      "ADMIN_TOKEN is set but no adminContext provided; admin endpoint NOT registered",
    );
  }
```

- [ ] **Step 4: Update `src/gateway/index.ts` to provide `adminContext`**

Add near the top:
```ts
import type { RouteConfig } from "../config/schemas.ts";
```

After the `server = Bun.serve(...)` call, build and inject `adminContext`. The order is delicate because `buildRoutes` is currently called inside the `Bun.serve(...)` call — restructure so the routes map is built once, the server captures it, AND the `adminContext.onReload` closure can later call `buildRoutes` again with the new routes when the admin endpoint reloads.

The cleanest restructure: build a helper inside `gateway/index.ts`:

```ts
function rebuildRoutes(currentRoutes: RouteConfig[] | undefined) {
  // If currentRoutes is passed, override config.routes for the next buildRoutes call.
  // For simplicity we don't actually mutate config; buildRoutes already reads
  // config.routes via the Proxy. After admin PUT, we reset the cache and reload.
  if (currentRoutes !== undefined) {
    // Set ROUTES_JSON so the next config read picks them up.
    process.env.ROUTES_JSON = JSON.stringify(currentRoutes);
    resetConfigCache();
  }
  return buildRoutes({
    producer,
    outbox: outboxWriter,
    adminContext: config.admin?.token && config.routesFile
      ? {
          routesFilePath: config.routesFile,
          onReload: (routes) => {
            const newRoutes = rebuildRoutes(routes);
            server.reload({ routes: newRoutes });
          },
        }
      : undefined,
  });
}
```

Then `const server = Bun.serve({ ..., routes: rebuildRoutes(undefined) });`.

Add an import: `import { resetConfigCache } from "../config/loader.ts";`

Add the startup log:
```ts
if (config.admin?.token) {
  if (config.routesFile) {
    log.info({ routesFilePath: config.routesFile }, "admin endpoint will be enabled");
  } else {
    log.warn("ADMIN_TOKEN set but ROUTES_FILE unset; admin endpoint disabled (cannot persist)");
  }
} else {
  log.info("admin endpoint disabled (ADMIN_TOKEN unset)");
}
```

**Important constraint:** the admin endpoint only registers when BOTH `ADMIN_TOKEN` AND `ROUTES_FILE` are set. Without persistence, a reload would be lost on next restart — that's a footgun we explicitly avoid. Update the test in Step 1 if needed to reflect this: the second test (`registers /admin/routes PUT when ADMIN_TOKEN is set`) already passes `routesFilePath` so it's fine; the third test would need a renaming to reflect that the missing piece is `adminContext` (the implementer's choice — the third test could become "without routesFile, the gateway logs a warning and skips registration").

- [ ] **Step 5: Verify the test passes**

Run: `bun test test/unit/gateway.index.adminEnabled.test.ts`
Expected: PASS.

- [ ] **Step 6: Full suite + typecheck**

Run: `bun test && bun run typecheck`
Expected: ALL PASS; clean.

- [ ] **Step 7: Commit**

```bash
git add src/gateway/routes.ts src/gateway/index.ts test/unit/gateway.index.adminEnabled.test.ts
git commit -m "$(cat <<'EOF'
SIO-803: gateway conditionally registers PUT /admin/routes

buildRoutes accepts an optional adminContext (routesFilePath + onReload
callback). The endpoint is registered iff both ADMIN_TOKEN and ROUTES_FILE
are set — without persistence the reload would be lost on restart, so we
refuse to register a footgun.

gateway/index.ts wires the admin onReload to write the file, reset the
config cache via process.env.ROUTES_JSON, rebuild routes, and call
server.reload({ routes }). Startup logs the admin endpoint state.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: CLAUDE.md updates

**Files:**
- Modify: `CLAUDE.md`

The CLAUDE.md change from the prior session (spec/plan linkage rule) is already in the working tree on this branch. This task folds it into a single CLAUDE.md commit alongside the admin-endpoint documentation.

- [ ] **Step 1: Verify the pre-existing change**

Run: `git diff CLAUDE.md`
Expected: the 6-line spec/plan linkage addition under the Linear Project section is visible.

- [ ] **Step 2: Add a new "Admin endpoint" subsection**

Find the `### Kafka topics` section. Immediately AFTER it (before `### Kafka provider factory`), add:

```markdown
### Admin endpoint (optional)

When `ADMIN_TOKEN` (min 32 chars) and `ROUTES_FILE` (path to a writable JSON file) are both set, the gateway registers `PUT /admin/routes` protected by an `X-Admin-Token` header. Body is the full routes array; on success it is validated by the same `routesSchema` used at startup, atomically written to `ROUTES_FILE`, and `server.reload({ routes })` swaps the live route map. Startup precedence for routes is `ROUTES_FILE > ROUTES_JSON > defaults`.

Without `ADMIN_TOKEN`, the endpoint is not registered (returns 404). Without `ROUTES_FILE`, it is also not registered — the gateway logs a warning and refuses to enable an in-memory-only admin surface that would be lost on restart.

`/healthz` and `/admin/routes` are reserved paths; any config route attempting to declare them fails startup with a Zod error.
```

- [ ] **Step 3: Update the env-var section under Critical Rules**

Find the existing env-var documentation (it's part of the Servers/ports table or near the Commands section — the implementer should locate it). Add:

| Env var | Purpose |
|---|---|
| `ADMIN_TOKEN` | Shared secret for `/admin/routes`. Min 32 chars. Endpoint disabled when unset. |
| `ROUTES_FILE` | Path to mounted JSON file holding the routes array. Required alongside `ADMIN_TOKEN` for the admin endpoint to register. Read on startup; takes precedence over `ROUTES_JSON`. |

If there is no existing env-var table, add a short bullet list under the Admin endpoint subsection above instead.

- [ ] **Step 4: Update the Out of scope section**

Find the existing "Out of scope (do not add without discussion)" list. Append after the existing items:

```
The previous deferrals of hot route reload (`server.reload()`), a mounted routes file, and admin-endpoint authentication are now addressed by SIO-803 (`docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md`). Webhook authentication for public webhook paths remains deferred.
```

- [ ] **Step 5: Run typecheck + tests (sanity)**

Run: `bun test && bun run typecheck`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
SIO-803: CLAUDE.md for spec/plan linkage rule + admin endpoint

(a) Linear Project section: every issue must include Spec: and Plan:
paths in its description, including follow-up tickets that reference
the parent spec.

(b) New Admin endpoint subsection documenting ADMIN_TOKEN, ROUTES_FILE,
reserved paths, and the ROUTES_FILE > ROUTES_JSON > defaults
precedence. Out-of-scope list updated to note which prior v1 deferrals
are now addressed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Pre-PR verification

- [ ] **Step 1: Full clean run**

Run:
```bash
bun test
bun run typecheck
```
Expected: all clean.

- [ ] **Step 2: Negative startup probes**

```bash
# Reserved path rejection
ROUTES_JSON='[{"name":"shadow","path":"/healthz","topic":"T_PRIVATE_SOURCE_X_Y","keyFields":["id"]}]' \
  ENVIRONMENT=dev KAFKA_PROVIDER=local \
  bun -e 'import("./src/config/loader.ts").then(m => { try { m.buildConfig(); console.log("UNEXPECTED OK"); } catch(e) { console.log("EXPECTED REJECTED:", e.message); } })'
# Expected: "EXPECTED REJECTED: ... path '/healthz' is reserved"

# ADMIN_TOKEN too short
ADMIN_TOKEN=short ENVIRONMENT=dev KAFKA_PROVIDER=local \
  bun -e 'import("./src/config/loader.ts").then(m => { try { m.buildConfig(); console.log("UNEXPECTED OK"); } catch(e) { console.log("EXPECTED REJECTED:", e.message); } })'
# Expected: "EXPECTED REJECTED: ... admin.token: ADMIN_TOKEN must be at least 32 characters"

# ROUTES_FILE missing file
ROUTES_FILE=/nonexistent/routes.json ENVIRONMENT=dev KAFKA_PROVIDER=local \
  bun -e 'import("./src/config/loader.ts").then(m => { try { m.buildConfig(); console.log("UNEXPECTED OK"); } catch(e) { console.log("EXPECTED REJECTED:", e.message); } })'
# Expected: "EXPECTED REJECTED: ENOENT" or similar

# ROUTES_FILE wins over ROUTES_JSON
TMPF=$(mktemp); echo '[{"name":"from-file","path":"/webhooks/file","topic":"T_PRIVATE_SOURCE_FILE_X","keyFields":["id"]}]' > $TMPF
ROUTES_FILE=$TMPF ROUTES_JSON='[{"name":"from-env","path":"/webhooks/env","topic":"T_PRIVATE_SOURCE_ENV_X","keyFields":["id"]}]' \
  ENVIRONMENT=dev KAFKA_PROVIDER=local \
  bun -e 'import("./src/config/loader.ts").then(m => console.log(m.buildConfig().routes[0].name))'
rm $TMPF
# Expected: "from-file"
```

All four probes must fail/succeed as expected. Capture the output for the PR description.

- [ ] **Step 3: Update Linear status to In Review (after PR push)**

The PR-push auto-flips the linked issue to Done — revert to In Review per the project rule.

- [ ] **Step 4: Open the PR**

```bash
git push -u origin SIO-803-reserved-paths-admin-routes
gh pr create --title "SIO-803: Reserved operational paths + admin routes endpoint" --body "$(cat <<'EOF'
## Summary
- Reserved-path guard: `routesSchema` rejects any route declaring `/healthz` or `/admin/routes` with a distinct Zod error.
- `PUT /admin/routes` admin endpoint (`X-Admin-Token` auth, Zod-validated full-replacement, atomic-write persistence to `ROUTES_FILE`, hot reload via `server.reload({ routes })`).
- Startup precedence for routes is now `ROUTES_FILE > ROUTES_JSON > defaults`.
- CLAUDE.md: new spec/plan linkage rule for Linear issues + admin endpoint documentation.

This explicitly overrides three v1 out-of-scope items from SIO-802's spec: webhook auth (for the admin endpoint only), hot reload, and mounted routes file. Webhook auth for public webhook paths remains deferred.

Linear: [SIO-803](https://linear.app/siobytes/issue/SIO-803)
Spec: `docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md`
Plan: `docs/superpowers/plans/2026-05-20-admin-routes-endpoint.md`

## Test plan
- [x] `bun test` clean
- [x] `bun run typecheck` clean
- [x] Negative startup probes (reserved path, short token, missing routes file, file > env precedence)
- [ ] Manual: smoke `PUT /admin/routes` against a running gateway with `ADMIN_TOKEN` + `ROUTES_FILE`, observe hot reload, restart, observe file replay.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Revert Linear from auto-Done to In Review**

Via the Linear MCP `save_issue` with `state: "In Review"` and `id: "SIO-803"`.

---

## Self-Review (against the spec)

**Spec coverage:**

| Spec section | Covered by |
|---|---|
| Reserved paths constant + helper | Task 1 |
| `routesSchema` rejects reserved paths | Task 2 |
| `verifyAdminToken` timing-safe | Task 3 |
| `readRoutesFile`/`writeRoutesFile` atomic | Task 4 |
| `admin.token` + `routesFile` config schema | Task 5 |
| `ROUTES_FILE > ROUTES_JSON > defaults` precedence | Task 6 |
| PUT handler (auth, validation, persist, reload, error matrix) | Task 7 |
| Conditional admin registration in gateway/index.ts | Task 8 |
| CLAUDE.md: linkage rule + admin docs + out-of-scope update | Task 9 |
| Verification probes | Task 10 |

**Placeholder scan:** No "TBD", "TODO", or "fill in" anywhere. Every code step shows the actual code.

**Type consistency:**
- `RouteConfig` exported from `src/config/schemas.ts` is reused by `makeAdminRoutesHandler.onReload` and `AdminContext.onReload`.
- `RESERVED_PATHS: ReadonlySet<string>` (Task 1) is used by Zod (Task 2) and read-only — never mutated.
- `verifyAdminToken(provided: string | null | undefined, expected: string)` (Task 3) is called by the endpoint handler with `req.headers.get("x-admin-token")` which returns `string | null` — compatible.
- `AdminRoutesDeps` (Task 7) and `AdminContext` (Task 8) both carry `routesFilePath` and `onReload` — keep the field names identical so plumbing is direct.
