# Getting Started

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

Local development loop for eventgate: bring up Redpanda and Couchbase via Docker Compose, run the gateway and writer in watch mode, send a sample webhook, and run the unit tests. This is the fastest path to a running stack on a developer laptop.

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
| 8091-8096 | Couchbase Server (UI on 8091) |
| 11210 | Couchbase memcached |

Confirm nothing else is listening on these ports before you start:

```bash
lsof -i :3000 -i :9092 -i :8091 -i :11210
```

## Bootstrap

```bash
bun install
cp .env.example .env
docker compose up -d
```

`docker compose up -d` starts:

- Redpanda single-node (Kafka API on 9092). The companion `redpanda-init` container auto-creates `ops.elastic.autoops.raw.v1`, `ops.elastic.autoops.events.v1`, and `ops.elastic.autoops.dlq.v1`.
- Couchbase Server 7.6 with a persistent volume.

## One-time Couchbase setup

Couchbase needs the bucket and collections created via the UI on the first run:

1. Open `http://localhost:8091`.
2. Log in with `Administrator` / `password` (the defaults from `docker-compose.yml`).
3. Create a bucket named `ops`.
4. Under the `_default` scope, create two collections: `autoops_events` and `autoops_state`.

These names match the defaults in `src/config/defaults.ts`; no `.env` change is required.

## Run the two processes

In two terminals:

```bash
# Terminal A
bun run dev:gateway

# Terminal B
bun run dev:writer
```

Both commands use `bun run --watch`, so saving a TypeScript file restarts the affected process. The gateway listens on `http://localhost:3000`; the writer connects to Redpanda as consumer group `autoops-couchbase-writer-v1` on `ops.elastic.autoops.events.v1`.

Expected log lines:

```
gateway listening { host: '0.0.0.0', port: 3000, topics: { raw: 'ops.elastic.autoops.raw.v1', events: 'ops.elastic.autoops.events.v1' } }
writer consuming { topic: 'ops.elastic.autoops.events.v1', groupId: 'autoops-couchbase-writer-v1' }
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
{
  "accepted": true,
  "resourceId": "r-123",
  "idempotencyKey": "<sha256-hex>"
}
```

Send the matching close to exercise the rolling state document:

```bash
curl -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceId": "r-123",
    "resourceName": "search-prod-eu",
    "title": "JVM memory pressure high",
    "severity": "High",
    "status": "close",
    "startTime": "2026-05-18T19:27:40Z",
    "endTime": "2026-05-18T19:42:11Z"
  }'
```

In the Couchbase UI Query workbench:

```sql
SELECT META().id, * FROM `ops`.`_default`.`autoops_events` LIMIT 10;
SELECT META().id, * FROM `ops`.`_default`.`autoops_state` LIMIT 10;
```

Expected: two history documents (open + close), one state document with `currentStatus: "closed"`, `openCount: 1`, `closeCount: 1`, `isOpen: false`.

## Run the unit tests

```bash
bun test
bun run typecheck
```

`bun test` runs with `LOG_LEVEL=silent` (set by `test/preload.ts`), so the output is just test results — no log noise. Single-file or single-test execution:

```bash
bun test test/unit/normalize.test.ts
bun test -t "buildIdempotencyKey"
```

## Configuration-aware probes

To exercise the production safety refinements without a real prod environment:

```bash
ENVIRONMENT=prod COUCHBASE_CONNSTR=couchbase://localhost bun run start:gateway
```

Expected: exit with a Zod refinement error citing `localhost` on `couchbase.connStr`. See [../configuration/environment-variables.md](../configuration/environment-variables.md) for the full list of refinements.

## Tear down

```bash
docker compose down
```

Add `-v` to also drop the Couchbase data volume:

```bash
docker compose down -v
```

Dropping the volume erases the `ops` bucket — the one-time setup steps above must be repeated next time.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `bun run dev:gateway` exits immediately with a Zod error | An env var is invalid or the production refinements tripped | Read the error path; usually `KAFKA_BROKERS`, `COUCHBASE_CONNSTR`, or `ENVIRONMENT=prod` set accidentally |
| `/healthz` returns `503` | Producer not connected | Confirm Redpanda is healthy: `docker compose ps` and `docker logs eventgate-redpanda` |
| Writer logs `consumer stream error` and exits | Redpanda restarted or the consumer group is in rebalance | The watch process will restart automatically; if it persists, restart Redpanda |
| `404` from the gateway | Wrong path | The only routes are `POST /webhooks/elastic/autoops` and `GET /healthz` |
| Couchbase upserts fail with auth errors | Credentials not updated after a fresh `docker compose up -d -v` | Re-create the `ops` bucket and collections in the UI |

## See Also

- [../architecture/overview.md](../architecture/overview.md) — what each process does and how they communicate.
- [../api/webhooks.md](../api/webhooks.md) — full request and response shapes.
- [../configuration/environment-variables.md](../configuration/environment-variables.md) — every environment variable the service reads.
- [../operations/logging.md](../operations/logging.md) — how to interpret the structured log output.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial getting-started doc created |
