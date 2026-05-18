# eventgate

A Bun-based ingestion service for [Elastic AutoOps](https://www.elastic.co/docs/deploy-manage/monitor/autoops) webhook notifications. Receives AutoOps POSTs, normalizes them, publishes to Kafka, and projects them into Couchbase for operational queries and trend analysis.

```
Elastic AutoOps  --POST-->  Bun gateway  --produce-->  Kafka  --consume-->  Couchbase
                                                       topics
                                                       - ops.elastic.autoops.raw.v1
                                                       - ops.elastic.autoops.events.v1
                                                       - ops.elastic.autoops.dlq.v1
```

Two processes:

- `gateway` (`src/gateway/index.ts`) — HTTP receiver at `POST /webhooks/elastic/autoops`. Validates, normalizes, publishes raw + normalized events to Kafka, returns 202.
- `writer` (`src/writer/index.ts`) — Kafka consumer that upserts append-only history docs and rolling state docs into Couchbase. Sends malformed messages to a DLQ topic.

## Quick start

```bash
bun install
cp .env.example .env
docker compose up -d
```

Create the Couchbase bucket and collections via the UI at http://localhost:8091 (default creds `Administrator` / `password`):

1. Create bucket `ops`.
2. Under the `_default` scope, create collections `autoops_events` and `autoops_state`.

Then in two terminals:

```bash
bun run dev:gateway
bun run dev:writer
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

Expect a `202` with `{ accepted, resourceId, idempotencyKey }`.

## AutoOps webhook contract

The connector lets you template the request body with these variables (per the [Notifications Settings docs](https://www.elastic.co/docs/deploy-manage/monitor/autoops/ec-autoops-notifications-settings)): `RESOURCE_ID`, `RESOURCE_NAME`, `TITLE`, `DESCRIPTION`, `SEVERITY` (`High|Medium|Low`), `STATUS` (`open|close`), `MESSAGE`, `START_TIME`, `END_TIME`, `ENDPOINT_TYPE`, `AFFECTED_NODES`, `AFFECTED_INDICES`, `EVENT_LINK`.

Configure the AutoOps connector body to map those to the camelCase fields this service expects:

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

## Couchbase document model

- `autoops_events` — append-only history, key `autoops::event::<resourceId>::<occurredAt>::<idempotencyKey>`. Source of truth for replay and trend analysis.
- `autoops_state` — rolling current state per alert, key `autoops::state::<resourceId>::<alertSignature>`. Tracks `currentStatus`, `openCount`, `closeCount`, `firstSeenAt`, `lastSeenAt`, `isOpen`.

`alertSignature` is `slugify(resourceId :: title)` so open + close events for the same alert roll up into one state doc.

`idempotencyKey` is `sha256(resourceId :: title :: status :: startTime :: endTime)` so retries of the same delivery are upserts and an open + close pair produce two distinct history docs.

## Tests

```bash
bun test
bun run typecheck
```

Tests cover the pure normalization and projection logic (`src/__tests__/`). Kafka and Couchbase integration are exercised manually via the smoke test above.

## Out of scope for v1

- **Webhook auth.** AutoOps webhook connectors expose only Name, URL, Method, custom Headers, and Body — no native HMAC/bearer/basic-auth. Operators should put the gateway behind ALB/Cloudflare Access or similar until v2 adds shared-token header validation.
- Flink rolling aggregates.
- Couchbase Kafka Sink Connector (custom consumer is sufficient and gives us projection control).
- Slack / PagerDuty fan-out consumers — added in a later phase as separate consumer groups on `ops.elastic.autoops.events.v1`.
- Dockerfile for gateway/writer — added once the deployment target is chosen.
