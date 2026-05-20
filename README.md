# eventgate

eventgate is a single-process Bun service that ingests webhook notifications from configured sources and durably persists them to Kafka. The gateway accepts any valid JSON POST on each configured route path and writes it verbatim to that route's Kafka topic via a local SQLite outbox. Routes are data — declared in `config.routes[]`, validated at startup against an org-wide topic naming policy. Validation, normalization, alerting, and projection are concerns for downstream consumers in other services.

> Full project documentation: [`docs/README.md`](docs/README.md). Architecture diagrams: [`docs/architecture/overview.md`](docs/architecture/overview.md) (system view) and [`docs/architecture/request-flows.md`](docs/architecture/request-flows.md) (sequence diagrams). Portable Bun programming guides: [`guides/`](guides/).

```
Webhook source  --POST-->  Bun gateway  --enqueue-->  SQLite outbox  --publish-->  Kafka T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>
```

Single process:

- `gateway` (`src/gateway/index.ts`) — HTTP receiver that builds its route map from `config.routes[]` at startup. Each configured route gets its own `POST` handler that accepts any valid JSON, enqueues into the SQLite outbox tagged with the route's topic, and returns 202. A background drainer in the same Bun process publishes pending rows to Kafka. When `ADMIN_TOKEN` and `ROUTES_FILE` are both set, the gateway also exposes `PUT /admin/routes` for runtime route changes (token auth, hot reload, atomic-write persistence).

Kafka topic naming is enforced at startup. Gateway-owned topics must match `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` (the seed Elastic AutoOps route uses `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`). Optional companion DLQs follow `DLQ_T_<topic>` but are never written by the gateway — they exist so downstream consumers can introspect the canonical DLQ name. `T_PUBLIC_*`, `T_PRIVATE_SINK_*`, `T_PRIVATE_*_RICH_NOTIFICATIONS`, `T_PRIVATE_*_EVENTS`, and Kafka/Confluent system prefixes are all rejected with a distinct startup error.

`/healthz` and `/admin/routes` are reserved paths — any config route trying to declare them fails startup.

Kafka backend selection (local Redpanda / AWS MSK / Confluent Cloud) is abstracted behind a `KafkaProvider` factory under `src/kafka/providers/`. See [`docs/architecture/kafka-provider-factory.md`](docs/architecture/kafka-provider-factory.md).

## Quick start

```bash
bun install
cp .env.example .env
docker compose up -d
docker exec eventgate-redpanda rpk topic create T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
bun run dev:gateway
```

Send a test payload:

```bash
curl -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceId": "r-123",
    "resourceName": "search-prod-eu",
    "title": "JVM memory pressure high",
    "severity": "High",
    "status": "open",
    "startTime": "2026-05-18T19:27:40Z"
  }'
```

Expect a `202` with `{ accepted: true }`. When the body is AutoOps-shaped, the gateway also sets an `idempotencyKey` Kafka header for downstream consumers to dedupe on.

Confirm the message landed in Kafka:

```bash
docker exec eventgate-redpanda rpk topic consume T_PRIVATE_SOURCE_ELASTIC_AUTOOPS -n 1
```

## Adding routes

A new vendor (Datadog, GitHub, PagerDuty, another tenant) is a configuration change, not a code change. Three sources, in priority order:

1. **`ROUTES_FILE`** — mounted JSON file. Required if you also want `PUT /admin/routes` enabled.
2. **`ROUTES_JSON`** — env var. Full array replaces the defaults.
3. **`src/config/defaults.ts`** — the checked-in seed.

A second route, for example:

```bash
ROUTES_JSON='[
  {"name":"elastic-autoops","path":"/webhooks/elastic/autoops","topic":"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS","dlqTopic":"DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS","keyFields":["resourceId","deployment-id"],"idempotency":"elastic-autoops"},
  {"name":"datadog-alerts","path":"/webhooks/datadog/alerts","topic":"T_PRIVATE_SOURCE_DATADOG_ALERTS","dlqTopic":"DLQ_T_PRIVATE_SOURCE_DATADOG_ALERTS","keyFields":["alert_id","id"]}
]' bun run dev:gateway
```

The only code change you'd ever need for a new route is a custom idempotency-key strategy (`src/gateway/idempotencyStrategies.ts`) — one function, one config reference. Otherwise routing is data.

For the runtime admin workflow (`PUT /admin/routes`) see [`docs/architecture/overview.md`](docs/architecture/overview.md) and [`docs/architecture/request-flows.md`](docs/architecture/request-flows.md).

## AutoOps webhook contract

The gateway does not validate the body shape — any valid JSON is accepted and forwarded to the route's topic. For reference, the AutoOps connector lets you template the request body with these variables (per the [Notifications Settings docs](https://www.elastic.co/docs/deploy-manage/monitor/autoops/ec-autoops-notifications-settings)): `RESOURCE_ID`, `RESOURCE_NAME`, `TITLE`, `DESCRIPTION`, `SEVERITY` (`High|Medium|Low`), `STATUS` (`open|close`), `MESSAGE`, `START_TIME`, `END_TIME`, `ENDPOINT_TYPE`, `AFFECTED_NODES`, `AFFECTED_INDICES`, `EVENT_LINK`.

A typical AutoOps connector body maps those to camelCase fields:

```json
{
  "resourceId": "RESOURCE_ID",
  "resourceName": "RESOURCE_NAME",
  "title": "TITLE",
  "description": "DESCRIPTION",
  "severity": "SEVERITY",
  "status": "STATUS",
  "message": "MESSAGE",
  "startTime": "START_TIME",
  "endTime": "END_TIME",
  "endpointType": "ENDPOINT_TYPE",
  "affectedNodes": "AFFECTED_NODES",
  "affectedIndices": "AFFECTED_INDICES",
  "eventLink": "EVENT_LINK"
}
```

When the body looks AutoOps-shaped, the `elastic-autoops` idempotency strategy computes `idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)` and emits it as a Kafka header. Downstream consumers may dedupe on it. Bodies that do not match this shape are still accepted and forwarded — they just do not get the header.

## Tests

```bash
bun test
bun run typecheck
```

Tests cover the outbox (writer, drainer, backoff, migration), the opportunistic idempotency-key helper and strategies registry, config validation (topic naming policy, reserved paths, ROUTES_FILE precedence, admin token), the Kafka provider factory, route dispatch, the admin endpoint (auth, validation, persistence, hot reload), and atomic-write file helpers (`test/unit/`). Kafka I/O is exercised manually via the smoke test above.

## Out of scope for v1

- **Webhook auth for public webhook paths.** AutoOps webhook connectors expose only Name, URL, Method, custom Headers, and Body — no native HMAC/bearer/basic-auth. Operators should put the gateway behind ALB/Cloudflare Access or similar until v2 adds shared-token header validation. The `/admin/routes` endpoint is the only authed endpoint today.
- Flink rolling aggregates.
- Any database integration in this repo — downstream consumers own their storage.
- Slack / PagerDuty fan-out consumers — added later as separate services consuming from the relevant `T_PRIVATE_SOURCE_*` topic.
- `DELETE` or `PATCH` on the admin endpoint — full-replacement `PUT` only.
- Multi-writer coordination on `ROUTES_FILE` — single-writer per file assumed.

## AWS deployment

Deploy scripts under `scripts/deploy/` stand up the gateway on ECS Fargate in `eu-central-1`, backed by MSK Serverless. The webhook URL is the ALB DNS name printed by `12-print-url.sh`.

See [`docs/deployment/aws-ecs.md`](docs/deployment/aws-ecs.md) for the target topology and [`scripts/deploy/README.md`](scripts/deploy/README.md) for the operator runbook. Note: as of SIO-795, the scripts still provision the older two-service shape (gateway + writer + Couchbase env vars); cleanup of the writer service is tracked separately.

To tear the stack down: `scripts/deploy/teardown.sh` (interactive confirmation).
