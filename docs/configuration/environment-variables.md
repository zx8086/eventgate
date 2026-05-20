# Environment Variables

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

Reference for every environment variable eventgate reads, with default and purpose. Configuration follows the 4-pillar pattern — defaults in `src/config/defaults.ts`, env-var → field mapping in `src/config/envMapping.ts`, validation in `src/config/schemas.ts`, lazy validation via `src/config/loader.ts`. Bun loads `.env` files automatically; no `dotenv` package is used.

For the project-agnostic 4-pillar pattern see [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md).

## How Configuration Resolves

1. `defaults.ts` provides a default for every field.
2. `envMapping.ts` reads the environment and returns a deep-partial overrides object. Empty strings and unparseable numbers are dropped.
3. `loader.ts` merges defaults with overrides and validates the result through `configSchema.safeParse()`.
4. `config` is a Proxy — validation runs the first time any field is read, not at module import. Tests call `resetConfigCache()` to re-run validation against new env.

Comma-separated lists are trimmed and empty entries are dropped.

## Application

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `dev` | One of `dev`, `staging`, `prod`, `test`. Triggers production safety refinements. |

## Server

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | HTTP port for `Bun.serve()`. |

## Kafka (all providers)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_PROVIDER` | `local` | One of `local`, `msk`, `confluent`. See [../architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md). |
| `KAFKA_CLIENT_ID` | `eventgate-gateway` | Client id for the producer. |

Topic names are declared per route in `config.routes[]` (see the Routes section below), not at the kafka block.

## Local provider

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_LOCAL_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated bootstrap brokers. |

## AWS MSK provider

Required when `KAFKA_PROVIDER=msk`.

| Variable | Default | Description |
|----------|---------|-------------|
| `MSK_REGION` | (unset) | AWS region of the cluster. Required. |
| `MSK_CLUSTER_ARN` | (unset) | Cluster ARN. Brokers are discovered via `GetBootstrapBrokersCommand`. One of `MSK_CLUSTER_ARN` / `MSK_BROKERS` required. |
| `MSK_BROKERS` | (unset) | CSV bootstrap brokers. Skips discovery. One of `MSK_CLUSTER_ARN` / `MSK_BROKERS` required. |
| `MSK_AUTH_MODE` | `iam` | `iam` (SASL/OAUTHBEARER + TLS) \| `tls` (TLS only) \| `none` (PLAINTEXT). |

## Confluent Cloud provider

Required when `KAFKA_PROVIDER=confluent`.

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFLUENT_BOOTSTRAP_SERVERS` | (unset) | Bootstrap servers (host:port[,host:port]). |
| `CONFLUENT_API_KEY` | (unset) | SASL/PLAIN username. |
| `CONFLUENT_API_SECRET` | (unset) | SASL/PLAIN password. |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | One of `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `silent`. Test suite forces `silent` via `test/preload.ts`. |

See [../operations/logging.md](../operations/logging.md) for output format and recommended levels per environment.

## Routes

The set of webhook routes (path → topic mapping, partition-key fields, idempotency strategy) loads from one of three sources at startup with precedence `ROUTES_FILE > ROUTES_JSON > defaults`. The seed route in `src/config/defaults.ts` covers Elastic AutoOps; everything else is operator-supplied.

| Variable | Default | Description |
|----------|---------|-------------|
| `ROUTES_JSON` | (unset) | JSON-encoded array of route objects. Each entry: `{name, path, topic, dlqTopic?, sourceHeader?, keyFields, idempotency?}`. Replaces (does not merge with) the seed route when set. Malformed JSON falls back silently to defaults; a valid array that fails Zod validation crashes the task at startup. |
| `ROUTES_FILE` | (unset) | Path to a JSON file containing the routes array. Read at startup; takes precedence over `ROUTES_JSON`. Required alongside `ADMIN_TOKEN` to enable the optional `PUT /admin/routes` runtime-mutation endpoint. **Not used in the Fargate deployment shape** — see [../deployment/task-definition-example.md](../deployment/task-definition-example.md). |
| `ADMIN_TOKEN` | (unset) | Shared secret protecting `/admin/routes`. Min 32 characters. Endpoint is not registered when unset, or when set without `ROUTES_FILE` (in-memory-only mutation would be lost on restart). |

Field-by-field route schema rules (topic prefix policy, DLQ shape, reserved paths, duplicate detection, registered idempotency strategies) live in `src/config/schemas.ts`. See [../superpowers/specs/2026-05-19-config-driven-routes-design.md](../superpowers/specs/2026-05-19-config-driven-routes-design.md) for the original design rationale and [../deployment/aws-ecs.md](../deployment/aws-ecs.md) for the single-vendor and multi-vendor `ROUTES_JSON` examples used in production.

## Outbox (SQLite durability layer)

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTBOX_ENABLED` | `true` | When `false`, the gateway publishes to Kafka inline (legacy behavior). Accepts `true|false|1|0|yes|no` (case-insensitive). |
| `OUTBOX_DB_PATH` | `./data/outbox.db` | SQLite database file. Use `:memory:` for ephemeral tests. Parent directory is created on startup. |
| `OUTBOX_BATCH_SIZE` | `100` | Maximum rows the drainer fetches per loop iteration. |
| `OUTBOX_BACKOFF_MAX_MS` | `600000` | Cap on exponential backoff between retries, in ms (default 10 minutes). |
| `OUTBOX_MAX_AGE_HOURS` | `24` | Rows older than this become `status='failed'` and stop retrying. |
| `OUTBOX_IDLE_POLL_MS` | `5000` | Drainer poll interval when the previous batch was empty. |
| `OUTBOX_BUSY_POLL_MS` | `250` | Drainer poll interval when the previous batch was full. |
| `OUTBOX_BACKLOG_WARN` | `50000` | Pending-row count above which the drainer logs a warn each iteration. |

See [../architecture/outbox.md](../architecture/outbox.md) for the full design.

## Production Safety Refinements

When `ENVIRONMENT=prod`, `src/config/schemas.ts` runs additional checks via `.superRefine()`:

| Rule | Reason |
|------|--------|
| `KAFKA_PROVIDER` must not be `local` | Prevents accidentally pointing at developer-laptop brokers in prod |
| `msk` requires `MSK_REGION` plus `MSK_CLUSTER_ARN` or `MSK_BROKERS` | Catches half-configured MSK env early at startup |
| `confluent` requires the full triplet | Catches missing API credentials early at startup |

Manually probe the refinements:

```bash
ENVIRONMENT=prod KAFKA_PROVIDER=local bun run start:gateway
ENVIRONMENT=prod KAFKA_PROVIDER=msk MSK_REGION=eu-central-1 bun run start:gateway
ENVIRONMENT=prod KAFKA_PROVIDER=confluent bun run start:gateway
```

All three should exit with a Zod refinement error.

## Local Development Defaults

`docker-compose.yml` advertises Redpanda on `localhost:9092`, which matches the local-provider default. A minimal `.env` for local development:

```bash
# .env
ENVIRONMENT=dev
LOG_LEVEL=debug
```

`.env.example` in the repo root holds the full set of variables a deployment is likely to override, with commented stanzas for MSK and Confluent. Treat `.env.example` as documentation only — do not commit real secrets to it.

## See Also

- [architecture/overview.md](../architecture/overview.md) — how each setting maps to runtime behaviour.
- [architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md) — the provider abstraction these env vars feed.
- [deployment/aws-ecs.md](../deployment/aws-ecs.md) — where the production environment block is set.
- [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md) — project-agnostic pattern reference.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial environment variable reference created |
| 2026-05-19 | Replaced Couchbase + KAFKA_BROKERS/AUTH/REGION with Kafka provider factory env vars (SIO-795) |
| 2026-05-19 | Added `OUTBOX_*` env vars for the SQLite outbox layer (SIO-799) |
| 2026-05-19 | Clarified that `KAFKA_TOPIC_EVENTS` / `KAFKA_TOPIC_DLQ` are reserved for future consumer services and not written by the gateway (SIO-801) |
| 2026-05-20 | Removed unused `TENANT` env var; nothing in `src/` ever read `config.app.tenant` (SIO-808) |
| 2026-05-20 | Removed legacy `KAFKA_TOPIC_RAW|EVENTS|DLQ` env vars and the `kafka.topics.*` config block; per-route `topic` is the only source of truth. Also fixed the inline-publish (`OUTBOX_ENABLED=false`) path so it publishes to `route.topic` instead of the dropped legacy topic (SIO-809). |
