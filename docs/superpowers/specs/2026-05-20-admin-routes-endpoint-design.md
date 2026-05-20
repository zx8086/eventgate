# Design: reserved operational paths + admin routes endpoint

**Date:** 2026-05-20
**Status:** Approved by user, ready for implementation plan
**Related:** Follows on from `2026-05-19-config-driven-routes-design.md` (SIO-802). Explicitly overrides three v1 out-of-scope items from that spec: webhook auth (for the admin endpoint only), hot reload via `server.reload()`, and a mounted `ROUTES_FILE`.

## Goal

Close two foot-guns introduced by config-driven routes and add operator self-service:

1. **Reserved-path guard.** Today `buildRoutes` iterates `config.routes` and assigns `routes[route.path] = ...`. The `/healthz` handler is registered before that loop in the same map, so a route configured with `path: "/healthz"` silently overwrites it. Same hazard will apply to `/admin/routes` once added. Validation must reject any route declaring a reserved path.

2. **Runtime route configuration without redeploy.** Today every route change requires editing `defaults.ts` or `ROUTES_JSON` in the deployment and restarting. Operators need to add a new webhook endpoint live. Add `PUT /admin/routes` — auth-protected, Zod-validated, full-replacement — that persists to a mounted file and hot-swaps via `server.reload({ routes })`.

## Why

The reserved-path issue is small but real. Before SIO-802, the route list was hardcoded in TypeScript and a developer-author shadowing `/healthz` would have been caught in code review. After SIO-802, operators can ship `ROUTES_JSON` per environment, and the first time someone fat-fingers `/healthz` into production we lose health-check observability with no warning. Catching this at startup with a distinct Zod error is the same shape as the existing duplicate-path / topic-policy rules.

The admin endpoint addresses a different pain. AutoOps connector onboarding is now data-driven, but the deployment process around that data is still a TypeScript-grade ceremony: edit env, redeploy, wait for rollout. For internal admin teams that already have token-protected access to the gateway's internal network, the friction is unjustified — the data is operator data, the validation is the same Zod schema either way, and Bun supports hot route reload natively (`server.reload({ routes })`).

This work explicitly takes three v1 deferrals off the table:
- **Webhook auth** stays deferred for the public webhook paths. Only `/admin/routes` is auth-protected.
- **Hot reload** is now used — but only for routes, only triggered by the admin endpoint.
- **Mounted routes file** is the persistence layer for admin mutations.

These three were deferred because they were unmotivated in v1; now they're load-bearing for the admin workflow and ship together.

## Non-goals

- **Re-introducing webhook auth.** Public webhook paths (`/webhooks/...`) remain unauthenticated; the v2 webhook-auth decision stands.
- **`DELETE /admin/routes/:name` or `PATCH`.** Full-replacement `PUT` only. Matches existing `ROUTES_JSON` "replace, don't merge" semantics.
- **Route-identity tracking.** Routes are identified by path (uniqueness already enforced). No GUIDs, no version numbers, no last-modified-by metadata.
- **Audit log of who changed what.** The request had the token; that's all we record. Operator identity is the deployment's concern.
- **Hot reload of any other config** (topics, Kafka provider, outbox settings). Only the routes array is mutable at runtime.
- **Multi-writer coordination.** Single gateway process per file. If two gateways share a file, last writer wins; do not deploy that way.
- **TLS / mTLS at the application layer.** Network-layer protection (ALB, Envoy, mesh) is assumed.

## Final contract

### Reserved paths

```ts
export const RESERVED_PATHS = new Set(["/healthz", "/admin/routes"]);
```

Any `routes[i].path` matching a reserved path → startup fails with a Zod issue: `path '/healthz' is reserved for operational use`. Reserved-path checking lives in a small helper (`src/config/reservedPaths.ts`) modelled on `src/config/topicPolicy.ts`.

### Admin endpoint

| Aspect | Decision |
|---|---|
| Path | `/admin/routes` (itself a reserved path) |
| Method | `PUT` only |
| Auth | Shared secret in `X-Admin-Token` header, compared with `crypto.timingSafeEqual` against `config.admin.token` (from `ADMIN_TOKEN` env, min 32 chars) |
| Enablement | Disabled unless `ADMIN_TOKEN` is set (no token → no endpoint registered, `PUT /admin/routes` returns 404) |
| Body shape | JSON array conforming to `routesSchema` (same as `ROUTES_JSON`) |
| Persistence | Writes to `config.routesFile` path (from `ROUTES_FILE` env) before reloading |
| Reload | `server.reload({ routes: buildRoutes(...) })` |
| Response codes | 200 (success, body = validated routes), 400 (Zod failure, body = issues), 401 (missing/wrong token), 500 (file write failure — does NOT reload) |
| File-write atomicity | Write to `<path>.tmp` then `rename()` to `<path>`. Bun.write + node:fs.rename. |

### Startup precedence

| Source | Wins when |
|---|---|
| `ROUTES_FILE` (mounted file) | The env var is set AND the file exists and parses |
| `ROUTES_JSON` (env var) | `ROUTES_FILE` is unset or absent; `ROUTES_JSON` is set |
| `defaults.routes` | Neither of the above |

`ROUTES_FILE` set but file missing or malformed → process refuses to start. Loud fail (matches existing Zod behavior on bad `ROUTES_JSON`).

`ROUTES_FILE` and `ROUTES_JSON` both set → file wins. Logged at startup so operators see which source took effect. Same loud-fail rule on file parse error.

## Config schema additions

```ts
// added to configSchema
admin: z.strictObject({
  token: z.string().min(32, "ADMIN_TOKEN must be at least 32 characters"),
}).optional(),
routesFile: z.string().min(1).optional(),
```

Adding `admin` is optional — gateways without an `ADMIN_TOKEN` skip endpoint registration entirely.

## Architecture impact

| Component | Change |
|---|---|
| `src/config/schemas.ts` | Add `admin` + `routesFile` to `configSchema`. Insert reserved-path check into `routesSchema.superRefine`. |
| `src/config/reservedPaths.ts` (NEW) | `RESERVED_PATHS` set + `checkReservedPath(path): Check`. |
| `src/config/envMapping.ts` | `ADMIN_TOKEN` → `admin.token`; `ROUTES_FILE` → `routesFile`. |
| `src/config/loader.ts` | `buildConfig`: if `routesFile` set, read JSON synchronously via `Bun.file(path).json()`. File wins over env. |
| `src/admin/auth.ts` (NEW) | `verifyAdminToken(req, expected): boolean` using `crypto.timingSafeEqual`. |
| `src/admin/routesEndpoint.ts` (NEW) | `makeAdminRoutesHandler({ getCurrentRoutes, applyRoutes, routesFilePath, token, deps })`. Returns a `PUT` handler. |
| `src/admin/routesFile.ts` (NEW) | `readRoutesFile(path)`, `writeRoutesFile(path, routes)` with atomic-write. |
| `src/gateway/index.ts` | If `config.admin?.token` set, register `/admin/routes` in the routes map. Capture `server` reference so the admin handler can call `server.reload({ routes })`. Re-run `buildRoutes(...)` on each reload with the new validated routes. Startup log line `admin endpoint enabled` or `admin endpoint disabled (ADMIN_TOKEN unset)`. |
| `src/gateway/routes.ts` | No change to the `for` loop — the reservation check now runs upstream in Zod, so by the time routes reach `buildRoutes` they are guaranteed not to collide. |

## Data flow

### Startup
1. `buildConfig` runs once (lazy Proxy first-touch).
2. If `routesFile` set: `Bun.file(path).json()` → routes array.
3. Else if `ROUTES_JSON` set: parse → routes array.
4. Else: `defaults.routes`.
5. Whole config (including `admin` and the resolved routes) validated by `configSchema` → `routesSchema` enforces reserved-path + naming + uniqueness rules.
6. `buildRoutes(deps)` produces the routes map; gateway starts. `/admin/routes` registered iff `admin.token` set.

### Admin PUT request
1. Receive `PUT /admin/routes` → check `X-Admin-Token` with `crypto.timingSafeEqual` against `config.admin.token`. Mismatch → 401.
2. Parse body as JSON. Failure → 400.
3. `routesSchema.safeParse(body)`. Failure → 400 with the Zod issues serialised to JSON.
4. Write to `ROUTES_FILE` atomically (tmp + rename). Failure → 500, no reload.
5. Build new routes map with the validated routes → `server.reload({ routes })`.
6. Return 200 with the validated route list (now active).

The admin handler holds a closure over:
- `server` (for `.reload(...)`)
- `routesFilePath` (where to persist)
- `expectedToken` (for auth)
- `deps` (`producer`, `outbox`) — to pass into the rebuilt `buildRoutes` call

In-flight requests against the OLD route handlers complete safely because each handler is a pure closure over its own frozen `route` (verified in handler.ts:25-81 — no shared mutable state).

## Error handling

| Failure | Response | Persistence side-effect |
|---|---|---|
| Token mismatch | 401 `{ error: "unauthorized" }` | none |
| Token missing | 401 `{ error: "unauthorized" }` | none |
| Body not JSON | 400 `{ error: "invalid JSON body" }` | none |
| Zod validation fail | 400 `{ error: "validation", issues: [...] }` | none |
| Tmp file write fail | 500 `{ error: "persist failed" }` | none — no reload |
| Rename fail | 500 `{ error: "persist failed" }` | possible orphan `<path>.tmp` — operator cleanup |
| Reload throws | 500 `{ error: "reload failed" }` | file IS written; restart fixes it |

The reload-throws case is the awkward one: the file has the new routes, but the live server still has the old ones. Logged at error level. Next process restart will pick up the file. The window is tiny because `server.reload()` rebuilds an in-memory map; the only realistic failure mode is a malformed `buildRoutes` call, which can't happen after Zod has accepted the input.

## Security model

This is the first auth code in the repo. Scope it tightly:

- **Single shared secret.** No JWT, no OAuth, no per-user accounts. The admin endpoint is for operator tooling, not end-user access.
- **Token is from env only.** No on-disk secret file. Operators inject via deployment env (Kubernetes secret, AWS Secrets Manager, etc.).
- **Timing-safe comparison.** `crypto.timingSafeEqual(Buffer.from(provided), Buffer.from(expected))`. Both buffers must be same length; precompute `expectedBuf` once and compare via a wrapper that early-returns on length mismatch with a non-leaking constant-time bail.
- **Token length floor.** 32 characters minimum (~128 bits if hex). Enforced by Zod.
- **No logging the token.** Pino bindings must never include the token. The auth helper takes the expected token by reference and never logs request headers wholesale.
- **No rate limiting in v1.** Endpoint is assumed network-protected. If the gateway is ever exposed publicly, add rate limiting before — that's a follow-up ticket, not in scope here.
- **CSRF not applicable.** Endpoint requires a custom header (`X-Admin-Token`), which browsers can't send cross-origin without explicit CORS — and we set no CORS headers on this path.

## File persistence model

- **Mounted file.** Operator provides `ROUTES_FILE=/etc/eventgate/routes.json` (or any writable path). Directory must exist and be writable by the gateway process.
- **Atomic write.** `Bun.write("<path>.tmp", JSON.stringify(routes, null, 2))` then `node:fs.rename("<path>.tmp", path)`. POSIX rename is atomic on same filesystem. Cross-filesystem renames will fall back to copy+unlink — caller's problem if `ROUTES_FILE` and `<path>.tmp` are on different mounts.
- **No file lock.** Single-writer assumption. If two gateway processes share the file, the second's write clobbers the first — and they'll diverge on `server.reload` because each holds its own in-memory copy. The non-goal "multi-writer coordination" makes this explicit.
- **Read-back on next start.** Loader reads the file fresh on every `buildConfig` call. The lazy Proxy guarantees this happens at first access.

## Verification

Unit tests (same patterns as SIO-802):

- `test/unit/config.reservedPaths.test.ts` — helper accepts non-reserved, rejects `/healthz`, `/admin/routes`, returns specific message.
- `test/unit/config.routes.reserved.test.ts` — `routesSchema` rejects each reserved path with the right Zod issue.
- `test/unit/admin.auth.test.ts` — `verifyAdminToken` returns true for exact match, false for mismatch, false for missing header, false for wrong-length token (no timing leak via early-return path).
- `test/unit/admin.routesFile.test.ts` — round-trip write + read; atomic rename leaves no `.tmp` on success; read of non-existent file throws a specific error.
- `test/unit/admin.routesEndpoint.test.ts` — 401 on missing/wrong token; 400 on bad JSON; 400 on Zod failure; 200 on success with persistence + reload side-effects observable via spies; reload-throws path leaves file written and returns 500.
- `test/unit/config.loader.routesFile.test.ts` — file > env > defaults precedence; missing file throws; malformed file throws.
- `test/unit/gateway.index.adminEnabled.test.ts` — gateway registers `/admin/routes` iff `ADMIN_TOKEN` set.

Integration probes (manual, in the plan's verification section):
- Curl `PUT /admin/routes` with valid token + replacement routes; observe 200; observe new route accepts traffic; restart gateway and confirm file replays the routes.
- Negative probes: missing token → 401; reserved path in body → 400 with `path '/healthz' is reserved` message; sink topic → 400 with naming-policy message.

## Out of scope

- Webhook authentication (still v2 for public webhook paths).
- `DELETE`/`PATCH` on admin endpoint.
- Audit log.
- Multi-writer file coordination.
- Rate limiting on the admin endpoint.
- TLS termination at the gateway.
- Hot reload of non-route config (topics, Kafka provider, outbox).
- A `GET /admin/routes` endpoint — `/healthz` already exposes the current route list.

## Acceptance criteria

- `RESERVED_PATHS` includes `/healthz` and `/admin/routes`; any config route declaring a reserved path is rejected at startup with a distinct Zod error.
- `ADMIN_TOKEN` env (min 32 chars) controls admin-endpoint registration. Unset → endpoint absent → 404 on any HTTP method.
- `PUT /admin/routes` with valid token + valid body: 200, file written atomically to `ROUTES_FILE`, `server.reload({ routes })` invoked with the new map.
- `PUT /admin/routes` with bad token: 401. With bad body: 400 (Zod issues). With persistence failure: 500.
- `ROUTES_FILE` > `ROUTES_JSON` > defaults precedence enforced; either source failing parsing prevents startup.
- All new unit tests pass; `bun run typecheck` clean.
- CLAUDE.md updated: spec/plan linkage rule (project rule); reserved paths + admin endpoint documented in the routing section; new env vars listed.
- The prior spec's "Out of scope" entries for hot reload + mounted file + webhook auth are annotated to point at this spec.
