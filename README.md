# eventgate

A Bun-based ingestion service for [Elastic AutoOps](https://www.elastic.co/docs/deploy-manage/monitor/autoops) webhook notifications. Receives AutoOps POSTs, normalizes them, and publishes to Kafka for downstream consumers.

> Full project documentation: [`docs/README.md`](docs/README.md). Portable Bun programming guides: [`guides/`](guides/).

```
Elastic AutoOps  --POST-->  Bun gateway  --produce-->  Kafka
                                                       topics
                                                       - ops.elastic.autoops.raw.v1
                                                       - ops.elastic.autoops.events.v1
                                                       - ops.elastic.autoops.dlq.v1
```

Single process:

- `gateway` (`src/gateway/index.ts`) — HTTP receiver at `POST /webhooks/elastic/autoops`. Validates, normalizes, publishes raw + normalized events to Kafka, returns 202.

Kafka backend selection (local Redpanda / AWS MSK / Confluent Cloud) is abstracted behind a `KafkaProvider` factory under `src/kafka/providers/`. See [`docs/architecture/kafka-provider-factory.md`](docs/architecture/kafka-provider-factory.md).

## Quick start

```bash
bun install
cp .env.example .env
docker compose up -d
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

Expect a `202` with `{ accepted, resourceId, idempotencyKey }`.

Confirm the message landed in Kafka:

```bash
docker exec eventgate-redpanda rpk topic consume ops.elastic.autoops.events.v1 -n 1
```

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

## Normalization

- `alertSignature = slugify(resourceId :: title)` — stable across the open + close pair of the same alert; useful for downstream rollup.
- `idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)` — stable across retries of the same delivery. Returned in the 202 body and emitted as a Kafka header.

## Tests

```bash
bun test
bun run typecheck
```

Tests cover the pure normalization logic, config validation, and the Kafka provider factory (`test/unit/`). Kafka I/O is exercised manually via the smoke test above.

## Out of scope for v1

- **Webhook auth.** AutoOps webhook connectors expose only Name, URL, Method, custom Headers, and Body — no native HMAC/bearer/basic-auth. Operators should put the gateway behind ALB/Cloudflare Access or similar until v2 adds shared-token header validation.
- Flink rolling aggregates.
- Any database integration in this repo — downstream consumers own their storage.
- Slack / PagerDuty fan-out consumers — added later as separate consumer groups on `ops.elastic.autoops.events.v1`.

## AWS deployment

Deploy scripts under `scripts/deploy/` stand up the gateway on ECS Fargate in `eu-central-1`, backed by MSK Serverless. The webhook URL is the ALB DNS name printed by `12-print-url.sh`.

See [`docs/deployment/aws-ecs.md`](docs/deployment/aws-ecs.md) for the target topology and [`scripts/deploy/README.md`](scripts/deploy/README.md) for the operator runbook. Note: as of SIO-795, the scripts still provision the older two-service shape (gateway + writer + Couchbase env vars); cleanup of the writer service is tracked separately.

To tear the stack down: `scripts/deploy/teardown.sh` (interactive confirmation).
