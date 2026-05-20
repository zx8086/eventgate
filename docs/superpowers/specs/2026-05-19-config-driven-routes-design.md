# Design: config-driven webhook routes

**Date:** 2026-05-19
**Status:** Draft, awaiting user review
**Related:** Builds on the accept-everything contract from `2026-05-19-accept-everything-gateway-design.md` (SIO-801) and the outbox introduced in SIO-799

## Goal

Let operators add new webhook ingestion endpoints — each routed to its own Kafka topic — without writing TypeScript. The gateway already does one thing well: accept any valid JSON, enqueue verbatim to a single topic, return 202. This design generalises the route layer so that the *set* of (path, topic) pairs is data, while preserving the existing per-request behaviour exactly. It also enforces the org-wide topic naming policy at config-validation time so that all gateway-owned streams comply by construction.

## Why

Today the gateway is hard-wired to a single route, `POST /webhooks/elastic/autoops` → `ops.elastic.autoops.raw.v1`. Onboarding a second source (Datadog alerts, GitHub webhooks, PagerDuty incidents, future Elastic AutoOps tenants on different topics) currently requires a TypeScript change for what is mechanically a configuration concern: pick a path, pick a topic, pick a partition key, optionally compute an idempotency hint. The work is identical every time, so it should not be code work every time.

The accept-everything contract makes this safe: because the gateway never inspects or validates the body, "another webhook source" is genuinely just another (path, topic) pair. There is no per-source business logic in the gateway to duplicate.

The org has a topic naming policy. The gateway is the source connector in that taxonomy — it receives external webhooks and writes inbound raw streams — so every gateway-owned topic must live under the `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` prefix, with an optional companion DLQ named `DLQ_T_<topic>`. Hard-coded topics in `defaults.ts` are easy to keep compliant by inspection; once topics are a config table that operators can extend via env, "compliant by inspection" stops scaling. Validating the policy at config-load time turns it from a convention into an enforced invariant.

## Non-goals

- **Webhook authentication.** Still deferred to v2 per the existing `Out of scope` list. When auth arrives, it will either be a uniform scheme expressible in config (e.g. shared-token header check) or it will force a redesign — that is the explicit decision point, and this design preserves it cleanly.
- **Parametric paths (`/webhooks/:vendor/:product`).** Bun supports URL params natively, but parametric paths force template-based topic names, which force runtime topic validation. Each route declares its full literal path and full literal topic in v1.
- **Topic-name templates.** No `"ops.{vendor}.{product}.raw.v1"`. Topic is a literal string per route.
- **Hot reload of routes (`server.reload()`).** Bun supports it; we will not use it in v1. Adding a route requires a restart, same as any other config change.
- **A mounted routes file (`ROUTES_FILE=/etc/eventgate/routes.json`).** Defaults + env override is enough. If a third source proves necessary, add it then.
- **Per-route validation, normalization, response shaping, or auth.** Any feature that cannot be expressed in the route schema below is a signal to push the feature into a downstream consumer, not to expand the schema.
- **Per-route rate limiting, body-size caps, dropping pings, splitting batched events.** Same reasoning.
- **Replacing the `KafkaProvider` factory or the outbox.** Both remain unchanged.

## Final route contract

| Inbound | Outbound |
|---|---|
| POST to any configured route path with valid JSON body | 202 `{ accepted: true }`, one row enqueued to outbox → that route's topic |
| POST to a configured route with non-JSON body | 400 `{ accepted: false, error: "invalid JSON body" }` |
| POST to an unconfigured path | 404 |
| GET `/healthz` | 200/503 with `{ producer, outbox }` shape (unchanged) |

Per-request behaviour is identical to today's single-route handler. The only change is that the *set* of handlers is built from `config.routes[]` at startup.

## Route schema

A route is defined by:

```ts
{
  name: string;            // "elastic-autoops" — used for logs and as default sourceHeader
  path: string;            // "/webhooks/elastic/autoops" — literal, must start with "/"
  topic: string;           // "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" — must match the naming policy
  dlqTopic?: string;       // optional; if set, must equal `DLQ_T_${topic}`
  sourceHeader?: string;   // Kafka header value tagging origin; defaults to name
  keyFields: string[];     // ["resourceId", "deployment-id"] — first non-empty string hit wins
  idempotency?: string;    // optional named strategy; references idempotencyStrategies registry
}
```

`config.routes` is a non-empty array of routes. The Elastic AutoOps route remains the seed entry in `defaults.ts`, renamed to comply with the policy (see Rollout).

### Topic naming policy

The gateway is a source connector. Every gateway-owned `topic` must follow:

```
T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>
```

where `<SYSTEM>` and `<ENTITY>` are uppercase ASCII segments (`[A-Z][A-Z0-9]*`), separated by underscores; `<ENTITY>` itself may contain additional `_`-separated segments. The full topic must match:

```
/^T_PRIVATE_SOURCE_[A-Z][A-Z0-9]*_[A-Z][A-Z0-9]*(_[A-Z][A-Z0-9]*)*$/
```

Kafka topic-length limit (249) and legal-character set are also enforced.

The following prefixes are explicitly **forbidden** for gateway routes, each with a distinct Zod error message so a misconfiguration is unambiguous:

| Forbidden prefix / pattern | Error message |
|---|---|
| `T_PUBLIC_*` | "gateway is not an MDM publisher; topic must start with T_PRIVATE_SOURCE_" |
| `T_PRIVATE_SINK_*` | "gateway is not a sink connector; topic must start with T_PRIVATE_SOURCE_" |
| `T_PRIVATE_*_RICH_NOTIFICATIONS` / `T_PRIVATE_*_EVENTS` | "internal event/notification streams are not gateway-owned; topic must start with T_PRIVATE_SOURCE_" |
| `DLQ_T_*` (as the `topic` field) | "DLQ topics are declared via dlqTopic, not topic" |
| `__*`, `_schemas`, `_confluent-*` | "system topic prefix; not gateway-writable" |

### DLQ field

`dlqTopic` is optional. The gateway itself never writes to DLQ topics — it is a source connector, not a consumer. The field exists so that:

- A route can declare the canonical DLQ name once, in the same place as its primary topic, eliminating drift in downstream code.
- `/healthz` and the startup banner can surface the (topic, dlqTopic) pair so operators see the full ownership picture.
- Downstream consumer services can introspect `config.routes` (or an equivalent shared registry) to discover the DLQ name they should publish to, rather than reconstructing it.

When `dlqTopic` is set, it **must** equal `"DLQ_T_" + topic` exactly. No other DLQ name is legal. This makes the relationship a function of `topic` alone — there is no naming question to answer at route-definition time, only a question of whether the route is DLQ-eligible at all. If a route declares `dlqTopic` that doesn't match, startup fails with: "dlqTopic must be 'DLQ_T_<topic>'; got '<value>'".

### Cross-field validation (at startup, via `superRefine`)

- `routes` is non-empty.
- All `routes[].path` values are unique. Two routes cannot share a path.
- All `routes[].topic` values are unique. Two routes writing to the same topic would silently merge streams; that is almost never intended and is trivially catchable here.
- All `routes[].dlqTopic` values, where present, are unique (mechanically follows from `topic` uniqueness, but enforced explicitly so the error message is clear).
- All `routes[].path` start with `"/"`.
- All `routes[].keyFields` arrays are non-empty.
- Each `routes[].idempotency`, when set, references a key in the strategies registry.
- Each `routes[].topic` matches the `T_PRIVATE_SOURCE_*` policy regex above and none of the forbidden patterns.
- Each `routes[].dlqTopic`, when set, equals `"DLQ_T_" + topic`.
- Total topic length (and dlqTopic length, when set) is ≤ 249 characters.

Validation runs at first-read of `config` (existing Proxy semantics). Invalid config → process refuses to start with a Zod error, same as every other config failure today.

## Idempotency strategies as a named registry

```
src/gateway/idempotencyStrategies.ts
```

A named map of pure functions `(body: unknown) => string | undefined`:

```ts
import { autoOpsIdempotencyKey } from "./idempotencyKey.ts";

export const idempotencyStrategies: Record<string, (body: unknown) => string | undefined> = {
  "elastic-autoops": autoOpsIdempotencyKey,
};
```

A route entry references a strategy by name (`"idempotency": "elastic-autoops"`). No strategy named → no `idempotencyKey` header attached. Adding a new strategy is one function + one config reference; the route layer itself doesn't change.

This is the *only* place the gateway accepts code rather than config. The decision is explicit: idempotency derivation is logic, not data, and pretending otherwise (e.g. `{ idempotency: { fields: [...], joiner: "::" } }`) creates a tiny programming language in JSON that grows badly.

`src/gateway/idempotencyKey.ts` continues to export `autoOpsIdempotencyKey` — it is no longer imported by `routes.ts` directly, only by the strategies registry.

## Config sources

Two sources, in priority order:

1. **`src/config/defaults.ts`** — the canonical route list, checked into the repo. v1 ships with the existing Elastic AutoOps route as the seed entry. Routes added via PR follow the existing review process.
2. **`ROUTES_JSON` env var** — a JSON-encoded array. When present, **replaces** the defaults array entirely (does not merge per-route). This is the operator escape hatch for environment-specific routes without a PR. Same merge semantics as today's `KAFKA_LOCAL_BOOTSTRAP_SERVERS` (csv → array): the env wholly overrides the default.

The "replace, don't merge" choice is deliberate. Merging per-route would require route identity (by `name`? by `path`?) and a precedence rule for partial overrides, which is config complexity that nobody has asked for. If an operator wants to keep Elastic AutoOps *and* add Datadog in production, `ROUTES_JSON` must include both. The shape is simple and the validator catches missing entries.

`config.routes` is read once at gateway startup. There is no runtime reload in v1.

## Outbox topic typing

`outboxTopicSchema` today is `z.enum(["raw"])`. With multiple destination topics, this becomes:

```ts
export const outboxTopicSchema = z.string().min(1).describe("Full Kafka topic name to publish to.");
```

The outbox row's `topic` column stores the full topic string (`"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"`, `"T_PRIVATE_SOURCE_DATADOG_ALERTS"`, ...), not a topic-family enum key. Two consequences:

- **Writer (`src/outbox/writer.ts`):** `enqueue({ topic: "T_PRIVATE_SOURCE_...", ... })` — caller passes the full topic; no enum mapping.
- **Drainer (`src/outbox/drainer.ts`):** publishes to the row's `topic` string directly via `producer.sendByTopic(topicString, ...)`. No need to consult `config.kafka.topics`.

At startup, the gateway validates that every `routes[].topic` is non-empty and well-formed; the outbox itself does not need to know the allowed topic set, only that it has a string. This is the right separation: the route layer owns "what topics exist," the outbox is a generic durable buffer.

**Migration of existing outbox rows:** the migration adds no new columns, but rows written before this change have `topic = "raw"`. A backfill statement in the migration rewrites those rows to `topic = "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"` (the renamed Elastic AutoOps topic, see Rollout). This is safe because pre-change deployments only had one route. See migration notes in the implementation plan.

## Route registration

`src/gateway/routes.ts` becomes:

```ts
export function buildRoutes(deps: RouteDeps) {
  const { producer, outbox } = deps;
  const routes: Record<string, unknown> = {
    "/healthz": healthHandler(deps),
  };
  for (const r of config.routes) {
    routes[r.path] = { POST: makeWebhookHandler(r, deps) };
  }
  return routes;
}
```

`makeWebhookHandler(route, deps)` is the existing handler from today's single-route implementation, parameterised on `route`. The body of the handler is the same — parse JSON, derive idempotency via the configured strategy, build headers (`source: route.sourceHeader ?? route.name`), pick partition key from `route.keyFields`, enqueue to outbox with `topic: route.topic`. No new branches.

### Partition-key picker

Today's `pickKey()` is hard-coded to `["resourceId", "deployment-id"]`. It becomes:

```ts
function pickKey(body: unknown, fields: readonly string[]): string {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "unkeyed";
  const b = body as Record<string, unknown>;
  for (const k of fields) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return "unkeyed";
}
```

Same semantics, fields injected.

## Components

```
src/
  config/
    defaults.ts             // adds routes: [...] with the Elastic AutoOps seed entry
    envMapping.ts           // adds ROUTES_JSON parsing (json -> array, replace semantics)
    schemas.ts              // adds routesSchema + cross-field superRefine
    index.ts                // unchanged (Proxy)
  gateway/
    routes.ts               // iterates config.routes; one handler per entry
    handler.ts              // NEW — makeWebhookHandler(route, deps) extracted from routes.ts
    idempotencyKey.ts       // unchanged — still exports autoOpsIdempotencyKey
    idempotencyStrategies.ts// NEW — { "elastic-autoops": autoOpsIdempotencyKey, ... }
  outbox/
    schemas.ts              // outboxTopicSchema becomes z.string()
    writer.ts               // accepts topic string; no enum mapping
    drainer.ts              // publishes to row.topic directly
    db.ts                   // migration: rewrite legacy topic="raw" rows to the seed route's topic
test/
  unit/
    config.routes.test.ts                  // NEW — schema, env override, dedup, validation
    gateway.routes.dispatch.test.ts        // NEW — verifies one handler per config route, correct topic dispatch
    gateway.idempotencyStrategies.test.ts  // NEW — registry resolution, missing-strategy fallback
    outbox.writer.test.ts                  // adapted — topic is a string
    outbox.drainer.test.ts                 // adapted — topic dispatch
```

## Data flow (per request, unchanged in spirit)

1. Bun matches the incoming `POST <path>` against the `routes` map built at startup.
2. The matched handler holds a closure over its `Route` config.
3. `await req.json()` — non-JSON → 400.
4. `pickKey(body, route.keyFields)` for the partition key.
5. `strategy = idempotencyStrategies[route.idempotency]` if set; compute key, attach as Kafka header.
6. `outbox.enqueue({ topic: route.topic, messageKey, payload, headers: { source: route.sourceHeader ?? route.name, idempotencyKey? } })`.
7. 202.

The drainer (unchanged in shape) reads pending rows and publishes each to its row's `topic`. The producer is provider-agnostic; the Kafka provider factory does not need to know about routes at all.

## Error handling

- **Invalid `ROUTES_JSON`** (malformed JSON, missing fields, duplicate paths/topics, unknown idempotency strategy) → Zod error at startup; process exits. No partial routes loaded.
- **Body is not valid JSON** → 400, same as today.
- **Outbox enqueue fails** → 500, same as today.
- **Drainer publish fails** → backoff and retry per existing outbox semantics; the topic string travels with the row, so retries publish to the correct topic.

## Worked example: adding Datadog alerts

`ROUTES_JSON`:

```json
[
  {
    "name": "elastic-autoops",
    "path": "/webhooks/elastic/autoops",
    "topic": "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    "dlqTopic": "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    "keyFields": ["resourceId", "deployment-id"],
    "idempotency": "elastic-autoops"
  },
  {
    "name": "datadog-alerts",
    "path": "/webhooks/datadog/alerts",
    "topic": "T_PRIVATE_SOURCE_DATADOG_ALERTS",
    "dlqTopic": "DLQ_T_PRIVATE_SOURCE_DATADOG_ALERTS",
    "keyFields": ["alert_id", "id"]
  }
]
```

Restart the gateway. `/webhooks/datadog/alerts` is now live. Zero TypeScript was changed. If Datadog later needs a custom idempotency strategy, that is the one place a code change is required: one function in `idempotencyStrategies.ts`, one `"idempotency": "datadog-alerts"` in config.

A misconfiguration like `"topic": "ops.datadog.alerts.raw.v1"` or `"topic": "T_PRIVATE_SINK_DATADOG_ALERTS"` causes the gateway to refuse to start with a Zod error pointing at the offending field — failure is loud, immediate, and at config-load time, not at first-publish.

## Testing strategy

- **Schema / config tests** (`test/unit/config.routes.test.ts`): defaults parse cleanly; `ROUTES_JSON` overrides; duplicate paths rejected; duplicate topics rejected; missing `keyFields` rejected; unknown `idempotency` strategy rejected; empty array rejected.
- **Topic naming policy tests** (`test/unit/config.routes.naming.test.ts`): `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` accepted; lowercase rejected; legacy `ops.*.raw.v1` rejected; each forbidden prefix (`T_PUBLIC_*`, `T_PRIVATE_SINK_*`, `T_PRIVATE_*_RICH_NOTIFICATIONS`, `T_PRIVATE_*_EVENTS`, `DLQ_T_*`, `__*`, `_schemas`, `_confluent-*`) rejected with the right message; topic length > 249 rejected; `dlqTopic` mismatched with `topic` rejected; `dlqTopic` correctly derived case is accepted.
- **Route dispatch tests** (`test/unit/gateway.routes.dispatch.test.ts`): a config with two routes registers two POST paths; each handler enqueues to its own topic; unmatched path returns 404; non-JSON body returns 400 for both routes.
- **Strategy registry tests** (`test/unit/gateway.idempotencyStrategies.test.ts`): named strategy resolves; absent strategy yields no header; existing AutoOps key derivation still works through the registry.
- **Outbox tests adapted**: writer accepts arbitrary topic string; drainer publishes to the row's topic.
- **Manual smoke**: extend `docs/development/getting-started.md` with a two-route example (Elastic AutoOps + a contrived second route to a `T_PRIVATE_SOURCE_EXAMPLE_EVENTS` topic created in Redpanda).

`bun run typecheck` and `bun test` after every step, as standard.

## Rollout

This work renames the existing topic in the same PR, so the cutover is coordinated rather than purely additive. The rename is intentional — the topic-naming policy is the load-bearing reason this design exists; grandfathering the old name would create a precedent that undermines the policy from day one.

1. **Provision the new topic in every environment** (Redpanda dev, MSK staging, MSK prod) ahead of the deploy: `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` and, optionally, `DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`. Topic creation stays an ops concern as it is today; just done in advance.
2. **Coordinate with downstream consumers** before the gateway deploy. Any consumer of `ops.elastic.autoops.raw.v1` must add `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` to its subscription list and be running before the gateway switches over. Consumers can dual-subscribe during the cutover window; the old topic will simply stop receiving new writes after the gateway deploys. A short list of known consumer services should be enumerated in the implementation plan.
3. **Land the schema, strategy registry, and outbox topic-typing changes** behind the existing single route — no behaviour change yet.
4. **Migrate the outbox** in the same PR: small `bun:sqlite` migration that rewrites legacy rows with `topic = "raw"` to `topic = "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"`. Any rows still pending at deploy time drain to the new topic, not the old one. This is the right behaviour because pending rows have never been published anywhere yet.
5. **Cut over `routes.ts`** to iterate `config.routes`, with the renamed seed entry in `defaults.ts`.
6. **Smoke locally** against Redpanda: hit `/webhooks/elastic/autoops`, confirm the message lands on `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`; confirm `/healthz` reports the new topic.
7. **Deploy.** From the deploy moment, the old `ops.elastic.autoops.raw.v1` topic stops receiving traffic. Consumers should already be reading from the new topic.
8. **Decommission `ops.elastic.autoops.raw.v1`** after the consumer-side migration is verified — separate ops ticket, not part of this PR.

The pre-existing `config.kafka.topics.{raw, events, dlq}` defaults become unused once `routes[]` is the source of truth for topic names. They stay in place for this PR (smaller blast radius) and are flagged as cleanup in a follow-up ticket.

## Risks and edge cases

| Risk | Likelihood | Mitigation |
|---|---|---|
| Operator sets `ROUTES_JSON` that drops Elastic AutoOps in production | Medium | Documented "replace, don't merge" semantics + a startup log line enumerating all registered routes by name, path, topic, dlqTopic. Existing AutoOps connector failures would surface within minutes. |
| Two routes pointed at the same topic (e.g. dev/test entries) | Medium | Caught by `superRefine` at startup. |
| Operator declares a topic that doesn't exist in the Kafka cluster | High | Out of scope — same risk as today's `KAFKA_TOPIC_*` env vars. The producer will error on first publish; outbox will retry; surfaced via `/healthz` and logs. Topic provisioning stays an ops concern. |
| Future contributor adds per-route auth / response shaping into `routes[]` schema | Medium | Spec explicitly forbids it; PR review point. The escape valve when auth lands is "push it downstream or redesign," not "extend the route schema." |
| Future contributor relaxes the topic-naming regex to accept a non-conforming legacy name | Medium | The policy is documented inline in `routesSchema` with a comment linking back to this spec. Test cases for each forbidden prefix lock in the rule. |
| Downstream consumer not migrated before the gateway cutover, drops messages | High | Coordinated rollout step 2 above — consumers dual-subscribe ahead of the deploy. Failure mode is loud (consumer sees no traffic on the old topic post-cutover) rather than silent. |
| Pending outbox rows at cutover get republished to the new topic with surprising headers | Low | Headers travel with the row unchanged; only the destination topic differs. Pending rows have never been published anywhere yet, so "to the new topic" is the only correct destination. |
| Migration backfill misclassifies legacy rows | Low | Pre-change deployments only had one route; the only legacy topic value is `"raw"`, and the backfill rewrites it to `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`. Verified in `db.ts` migration test. |

## Out of scope

- Webhook authentication (deferred to v2 of this gateway design).
- Parametric paths and topic templates.
- Hot reload (`server.reload()`).
- Mounted routes file.
- Per-route auth, validation, normalization, body filtering, response shaping, rate limiting, size caps.
- A CLI for replaying outbox rows.
- Per-route metrics dashboards (the existing structured logs already include `component`, `topic`, `messageKey`; downstream observability owns dashboards).

## Acceptance criteria

- `config.routes` exists, is validated by Zod, and is overridable via `ROUTES_JSON`.
- Adding a new webhook endpoint requires only a `ROUTES_JSON` edit (or a one-entry PR in `defaults.ts`) — no TypeScript changes, unless the new source needs a custom idempotency strategy.
- The existing Elastic AutoOps per-request behaviour is preserved: same path, same headers, same partition-key logic, same idempotency-key derivation. The destination topic is renamed from `ops.elastic.autoops.raw.v1` to `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` as part of this work.
- Every `routes[].topic` is validated against the `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` policy; each forbidden prefix is rejected with a distinct error message; topic length ≤ 249.
- `routes[].dlqTopic`, when set, is exactly `"DLQ_T_" + topic`; any other value fails startup.
- The outbox stores and dispatches the full topic string per row; legacy rows with `topic = "raw"` are backfilled to `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`.
- All new and adapted unit tests pass; `bun run typecheck` is clean.
- A two-route smoke test against local Redpanda demonstrates traffic landing on both topics with the expected headers.
- Downstream consumers of the Elastic AutoOps stream are migrated to the new topic name before the gateway deploy.

## Field-set hardening (2026-05-20, SIO-810)

When this design landed, three route fields were marked optional: `dlqTopic`, `sourceHeader`, and `idempotency`. With multi-source onboarding now real, every field has become mandatory. The `config.routes[]` shape is `{name, path, topic, dlqTopic, sourceHeader, keyFields, idempotency}` — no optional fields. Operators get a clear Zod error at startup if any field is missing. Adding a new vendor now requires registering an idempotency strategy in `src/gateway/idempotencyStrategies.ts` before the route can be accepted.
