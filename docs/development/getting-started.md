# Getting Started

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-20
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

Local development loop for eventgate: bring up Redpanda via Docker Compose, run the gateway in watch mode, send a sample webhook, and run the unit tests. This is the fastest path to a running stack on a developer laptop.

## Prerequisites

| Tool | Minimum version | Check |
|------|-----------------|-------|
| Bun | 1.3.11 | `bun --version` |
| Docker | recent | `docker --version` |
| Docker Compose | v2 (`docker compose`, not `docker-compose`) | `docker compose version` |
| `curl` | any | `curl --version` |

Ports used by the local stack:

| Port | Service |
|------|---------|
| 3000 | gateway (`Bun.serve`) |
| 9092 | Redpanda (Kafka API) |
| 8082 | Redpanda (Pandaproxy) |
| 9644 | Redpanda (admin API) |

Confirm nothing else is listening on these ports before you start:

```bash
lsof -i :3000 -i :9092 -i :8082
```

## Bootstrap

```bash
bun install
cp .env.example .env
docker compose up -d
```

`docker compose up -d` starts:

- Redpanda single-node (Kafka API on 9092). The companion `redpanda-init` container auto-creates `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` — the topic the seed route in `src/config/defaults.ts` publishes to.

No database service is started — eventgate publishes only to Kafka. To add a new vendor, declare its topic in `ROUTES_JSON` / `ROUTES_FILE` and create it manually with `rpk -X brokers=localhost:9092 topic create <topic>`. The companion DLQ topic (`DLQ_T_<topic>`) is declared on each route but never written by the gateway, so it does not need to be pre-created here.

## Run the gateway

```bash
bun run dev:gateway
```

This uses `bun run --watch`, so saving a TypeScript file restarts the gateway. It listens on `http://localhost:3000`.

Expected log lines (one line per record; pretty-printed here for readability):

```
kafka provider selected { provider: 'Local Kafka', providerType: 'local' }
outbox enabled { dbPath: './data/outbox.db', batchSize: 100 }
admin endpoint disabled (ADMIN_TOKEN unset)
gateway listening {
  host: '0.0.0.0',
  port: 3000,
  routes: [ { name: 'elastic-autoops', path: '/webhooks/elastic/autoops', topic: 'T_PRIVATE_SOURCE_ELASTIC_AUTOOPS' } ]
}
```

## Send a test webhook

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

Expected response:

```json
{ "accepted": true }
```

Confirm the message landed in Kafka:

```bash
docker exec eventgate-redpanda rpk topic consume T_PRIVATE_SOURCE_ELASTIC_AUTOOPS -n 1
```

The gateway stamps an opportunistic `idempotencyKey` header on the Kafka record when it can derive one from the body — see [../api/webhooks.md](../api/webhooks.md#idempotency-header). The HTTP response itself is always just `{ accepted: true }`.

## Run the unit tests

```bash
bun test
bun run typecheck
```

`bun test` runs with `LOG_LEVEL=silent` (set by `test/preload.ts`), so the output is just test results — no log noise. Single-file or single-test execution:

```bash
bun test test/unit/outbox.writer.test.ts
bun test -t "deriveIdempotencyKey"
```

## Configuration-aware probes

To exercise the production safety refinements without a real prod environment:

```bash
ENVIRONMENT=prod KAFKA_PROVIDER=local bun run start:gateway
ENVIRONMENT=prod KAFKA_PROVIDER=msk MSK_REGION=eu-central-1 bun run start:gateway
ENVIRONMENT=prod KAFKA_PROVIDER=confluent bun run start:gateway
```

All three should exit with a Zod refinement error. See [../configuration/environment-variables.md](../configuration/environment-variables.md) for the full list of refinements.

## Tear down

```bash
docker compose down
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `bun run dev:gateway` exits immediately with a Zod error | An env var is invalid or the production refinements tripped | Read the error path; usually `KAFKA_PROVIDER` is set without the matching provider-specific vars, or `ENVIRONMENT=prod` is set accidentally |
| `/healthz` returns `503` | Producer not connected | Confirm Redpanda is healthy: `docker compose ps` and `docker logs eventgate-redpanda` |
| `404` from the gateway | Wrong path | Default routes are `POST /webhooks/elastic/autoops` and `GET /healthz`. Add more by setting `ROUTES_JSON` / `ROUTES_FILE`. `PUT /admin/routes` is registered only when both `ADMIN_TOKEN` (min 32 chars) and `ROUTES_FILE` are set. |
| Gateway logs `kafka publish failed` repeatedly | Brokers unreachable | Check `KAFKA_BROKERS` matches the Redpanda advertised listener |

## See Also

- [../architecture/overview.md](../architecture/overview.md) — what the gateway does and where its output goes.
- [../architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md) — how to switch the gateway between local Redpanda, MSK, and Confluent.
- [../api/webhooks.md](../api/webhooks.md) — full request and response shapes.
- [../configuration/environment-variables.md](../configuration/environment-variables.md) — every environment variable the service reads.
- [../operations/logging.md](../operations/logging.md) — how to interpret the structured log output.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial getting-started doc created |
| 2026-05-19 | Removed Couchbase bootstrap and writer process; gateway-only loop (SIO-795) |
| 2026-05-19 | Smoke test now consumes `raw.v1`; expected response shrunk to `{ accepted: true }` under accept-everything contract (SIO-801) |
| 2026-05-20 | Topics flow from `config.routes[]`; `redpanda-init` now creates `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`; startup log shows the registered route list; admin endpoint mentioned in 404 troubleshooting (SIO-802 / SIO-803 / SIO-807..SIO-811 cleanup, SIO-812) |
