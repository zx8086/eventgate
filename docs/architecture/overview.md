# Architecture Overview

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate is a single-process Bun service that ingests Elastic AutoOps webhook notifications and durably persists them to Kafka. The gateway accepts any valid JSON POST and writes it verbatim to `raw.v1` via a local SQLite outbox. Validation, normalization, alerting, and projection are concerns for downstream consumers in other services.

## Data Flow

```
+------------------+      POST        +------------------+   enqueue     +------------------+    publish    +-------------+
|  Elastic AutoOps | ---------------> |  gateway         | -----------> |  outbox (SQLite) | ----------> | Kafka raw.v1|
|  (webhook)       | <---  202 ack -- |  Bun.serve :3000 |              |  ./data/outbox.db|             +-------------+
+------------------+                  +------------------+              +------------------+
```

### Reserved topics

`ops.elastic.autoops.events.v1` and `ops.elastic.autoops.dlq.v1` stay provisioned but are not written by the gateway. They are reserved for future consumer services that may decide to publish normalized events or quarantine bad messages. The gateway has no opinion about validity — that is a consumer concern.

## Process

| Process | Entry point | Responsibility |
|---------|------------|----------------|
| gateway | `src/gateway/index.ts` | Accept any valid JSON POST, enqueue verbatim into the SQLite outbox, return `202`. Reports producer health on `/healthz`. |

There is no writer or projector in this repo. Add new downstream behaviour (Slack, PagerDuty, rolling aggregates, sinks into a database) as a **separate consumer service** on `raw.v1` (or a future `events.v1` published by a downstream normalizer) — never bolt it into the gateway.

## Kafka Topics

| Topic | Producer | Purpose |
|-------|----------|---------|
| `ops.elastic.autoops.raw.v1` | gateway | Verbatim webhook body, retained for replay. The gateway opportunistically sets an `idempotencyKey` Kafka header when the body looks AutoOps-shaped. |
| `ops.elastic.autoops.events.v1` | — | Reserved for future consumers that may publish normalized events. Not written by the gateway. |
| `ops.elastic.autoops.dlq.v1` | — | Reserved for future consumers that may quarantine bad messages. Not written by the gateway. |

## Kafka Provider Factory

The gateway connects to Kafka through a provider abstraction selected by `KAFKA_PROVIDER`:

| Provider | Auth | When |
|----------|------|------|
| `local` | PLAINTEXT | Local development against Redpanda |
| `msk` | OAUTHBEARER (IAM), TLS, or PLAINTEXT | AWS MSK / MSK Serverless |
| `confluent` | SASL/PLAIN over TLS | Confluent Cloud |

See [kafka-provider-factory.md](kafka-provider-factory.md) for env vars, field requirements, and the portable pattern.

## Failure Handling

| Failure | Where | Action |
|---------|-------|--------|
| Invalid JSON body | gateway (`src/gateway/routes.ts`) | Return `400`, do not enqueue |
| Outbox enqueue fails (SQLite) | gateway | Return `500`; the row is not durable, so the caller learns the truth |
| Kafka publish failure (drainer) | outbox drainer | Bump attempts, exponential backoff, eventually `status='failed'` after `OUTBOX_MAX_AGE_HOURS` |
| Producer disconnected | gateway `/healthz` | Report `producer.connected: false`, return `503` |

## Configuration

Configuration follows the 4-pillar pattern — defaults, env mapping, schema, loader — and is documented in [../configuration/environment-variables.md](../configuration/environment-variables.md). Production safety rules are enforced by `.superRefine()` in `src/config/schemas.ts`:

- `provider=local` is rejected when `ENVIRONMENT=prod` (prod must use `msk` or `confluent`).
- `msk` requires `MSK_REGION` plus one of `MSK_CLUSTER_ARN` / `MSK_BROKERS`.
- `confluent` requires `CONFLUENT_BOOTSTRAP_SERVERS`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET`.

For the project-agnostic 4-pillar pattern see [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md). For the project-agnostic provider factory pattern see [../../guides/kafka-provider-factory.md](../../guides/kafka-provider-factory.md).

## Out of Scope

These are documented separately in `CLAUDE.md` and the v1 plan; flagged here so readers know not to look for them in this overview:

- Webhook authentication (deferred to v2 once a shared-token header is agreed).
- Flink rolling aggregates.
- OpenTelemetry instrumentation.
- Any database integration in this repo — downstream consumers own their storage.

## See Also

- [kafka-provider-factory.md](kafka-provider-factory.md) — the provider abstraction the gateway uses to reach Kafka.
- [../api/webhooks.md](../api/webhooks.md) — request and response shapes for the gateway endpoints.
- [../deployment/aws-ecs.md](../deployment/aws-ecs.md) — how the gateway is deployed.
- [../operations/logging.md](../operations/logging.md) — log shape and filter patterns.
- [../plans/v1-implementation-plan.md](../plans/v1-implementation-plan.md) — original design rationale (historical; predates Couchbase removal).

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial architecture overview created |
| 2026-05-19 | Rewritten for gateway-only architecture: removed writer + Couchbase doc model, added Kafka provider factory layer (SIO-795) |
| 2026-05-19 | Accept-everything contract: gateway no longer validates or normalizes; only `raw.v1` is written; `events.v1` and `dlq.v1` reserved for future consumers (SIO-801) |
