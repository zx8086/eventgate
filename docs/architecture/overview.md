# Architecture Overview

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate is a two-process Bun service that ingests Elastic AutoOps webhook notifications, durably publishes them to Kafka, and projects them into Couchbase. The gateway and writer never call each other in-process — Kafka is the only coupling — which lets downstream consumers (alerting, aggregates, fan-out) be added as separate consumer groups without touching the ingestion path.

## Data Flow

```
+------------------+      POST       +------------------+    produce    +------------------+
|                  | --------------> |                  | ------------> |                  |
|  Elastic AutoOps |                 |  gateway         |               |  Kafka           |
|  (webhook)       |                 |  Bun.serve       |               |  raw.v1          |
|                  | <--- 202 ack -- |  port 3000       |               |  events.v1       |
+------------------+                 +------------------+               |  dlq.v1          |
                                                                        +--------+---------+
                                                                                 |
                                                                                 | consume
                                                                                 v
                                                                        +------------------+
                                                                        |                  |
                                                                        |  writer          |
                                                                        |  Kafka consumer  |
                                                                        |  + projector     |
                                                                        |                  |
                                                                        +--------+---------+
                                                                                 |
                                                                                 | upsert
                                                                                 v
                                                                        +------------------+
                                                                        |                  |
                                                                        |  Couchbase       |
                                                                        |  autoops_events  |
                                                                        |  autoops_state   |
                                                                        |                  |
                                                                        +------------------+
```

## Process Boundaries

| Process | Entry point | Responsibility |
|---------|------------|----------------|
| gateway | `src/gateway/index.ts` | Validate, normalize, publish raw + normalized events, return `202`. Reports producer health on `/healthz`. |
| writer | `src/writer/index.ts` | Consume `ops.elastic.autoops.events.v1`, upsert history doc + rolling state doc, DLQ malformed messages. |

Both processes are built from one container image. The split between them is enforced at the AWS ECS task-definition layer by overriding the container command — see [../deployment/aws-ecs.md](../deployment/aws-ecs.md).

## Kafka Topics

| Topic | Producer | Consumers | Purpose |
|-------|----------|-----------|---------|
| `ops.elastic.autoops.raw.v1` | gateway | (none in v1) | Verbatim webhook body, keyed by `resourceId`, retained for replay |
| `ops.elastic.autoops.events.v1` | gateway | writer | Normalized event, keyed by `resourceId`; headers carry `source`, `eventType`, `severity`, `schemaVersion`, `idempotencyKey` for filter-without-parse |
| `ops.elastic.autoops.dlq.v1` | writer | (none in v1) | Quarantined messages that failed JSON parse or missing-field validation |

Add new downstream behaviour (Slack, PagerDuty, rolling aggregates) as a **separate consumer group** on `events.v1` — never bolt it into the writer.

## Couchbase Document Model

| Collection | Doc key pattern | Shape |
|------------|-----------------|-------|
| `autoops_events` | `autoops::event::<resourceId>::<occurredAt>::<idempotencyKey>` | Append-only history, one doc per delivery (`AutoOpsEventHistoryDoc` in `src/types.ts`) |
| `autoops_state` | `autoops::state::<resourceId>::<alertSignature>` | Rolling per-alert state with `currentStatus`, `openCount`, `closeCount`, `firstSeenAt`, `lastSeenAt`, `isOpen` (`AutoOpsStateDoc` in `src/types.ts`) |

The key formats are produced by `historyDocKey()` and `stateDocKey()` in `src/couchbase/projection.ts`. The state document is built by `evolveState()` in the same file — that function is the **only** place an `AutoOpsStateDoc` is constructed.

## Normalization Contract

The lenient inbound schema lives in `src/gateway/schema.ts`; the pure normalization function is `normalizeElasticAutoOps()` in `src/normalize.ts`.

| Field | Rule | Reason |
|-------|------|--------|
| `severity` | Lowercase, map `high|medium|low`; anything else → `"unknown"` | AutoOps docs spell these `High|Medium|Low`, operators sometimes lowercase |
| `status` | Lowercase, map both `open`/`opened` → `"opened"`, `close`/`closed`/`resolved` → `"closed"` | AutoOps docs say `open|close`; templates and the resolved-event flow use other spellings |
| `alertSignature` | `slugify(resourceId :: title)` | Groups an open + close pair of the same alert into one state doc |
| `idempotencyKey` | `sha256(resourceId :: title :: status :: startTime :: endTime)` | Retries upsert the same history doc; an open + close pair produces two distinct history docs because `status` differs |
| `affectedNodes` / `affectedIndices` | CSV string or `string[]` accepted; coerced to `string[]` | AutoOps templates emit either format |

### Hyphenated keys and synthetic test bodies

`src/gateway/schema.ts` maps AutoOps' hyphenated default keys (`deployment-id`, `start-time`, ...) to camelCase before validation, and detects AutoOps' "Validate" button test body (every value is an un-substituted `${VAR}` placeholder) so the gateway can acknowledge it with `202` instead of failing schema validation.

## Idempotency Guarantee

The writer must stay idempotent — Kafka retries and consumer rebalances will re-deliver messages.

| Mutation | Idempotent because |
|----------|--------------------|
| `cb.history.upsert(historyDocKey(event), ...)` | Same `idempotencyKey` produces the same key; upsert overwrites with identical content |
| `cb.state.upsert(stateDocKey(event), evolveState(previous, event))` | `evolveState` reads the previous state document before writing; replays increment `openCount`/`closeCount` only when `previous.lastEventId` differs from `event.idempotencyKey` in practice (current code increments unconditionally — see Out of Scope) |

Do not add side effects that are not safe to replay. New behaviour belongs in a separate consumer group, not in the writer.

## Failure Handling

| Failure | Where | Action |
|---------|-------|--------|
| Invalid JSON body | gateway (`src/gateway/routes.ts`) | Return `400`, do not publish |
| Schema validation failure | gateway | Return `400` with Zod issues, do not publish |
| Kafka publish failure | gateway | Log a warning with `{ err, resourceId }`, still return `202` (writer is non-blocking) |
| Producer disconnected | gateway `/healthz` | Return `503` |
| Message JSON parse / missing fields | writer (`src/writer/index.ts`) | Produce to `ops.elastic.autoops.dlq.v1` with the reason, commit offset |
| Couchbase transient error | writer | Throw — the consumer surfaces the error and the message is re-delivered |
| Couchbase disabled (`COUCHBASE_ENABLED=false`) | writer | Log the normalized event and return; no upsert |

## Configuration

Configuration follows the 4-pillar pattern — defaults, env mapping, schema, loader — and is documented in [../configuration/environment-variables.md](../configuration/environment-variables.md). Production safety rules (no `localhost` brokers, TLS-only Couchbase, no default password, IAM auth on MSK) are enforced by `.superRefine()` in `src/config/schemas.ts`.

For the project-agnostic 4-pillar pattern see [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md).

## Out of Scope

These are documented separately in `CLAUDE.md` and the v1 plan; flagged here so readers know not to look for them in this overview:

- Webhook authentication (deferred to v2 once a shared-token header is agreed).
- Flink rolling aggregates.
- The Couchbase Kafka Sink Connector.
- OpenTelemetry instrumentation (Phase 3).
- Couchbase Capella hardening — TLS-only enforcement at runtime, timeout profiles, `AmbiguousTimeoutError` handling, circuit breaker (Phase 4).

## See Also

- [api/webhooks.md](../api/webhooks.md) — request and response shapes for the gateway endpoints.
- [deployment/aws-ecs.md](../deployment/aws-ecs.md) — how the two processes are deployed.
- [operations/logging.md](../operations/logging.md) — how to distinguish gateway and writer log output.
- [plans/v1-implementation-plan.md](../plans/v1-implementation-plan.md) — original design rationale.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial architecture overview created |
