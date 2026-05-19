# Environment Variables

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

Reference for every environment variable eventgate reads, with default, scope, and which process needs it. Configuration follows the 4-pillar pattern — defaults in `src/config/defaults.ts`, env-var → field mapping in `src/config/envMapping.ts`, validation in `src/config/schemas.ts`, lazy validation via `src/config/loader.ts`. Bun loads `.env` files automatically; no `dotenv` package is used.

For the project-agnostic 4-pillar pattern see [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md).

## How Configuration Resolves

1. `defaults.ts` provides a default for every field.
2. `envMapping.ts` reads the environment and returns a deep-partial overrides object. Empty strings and unparseable numbers are dropped.
3. `loader.ts` merges defaults with overrides and validates the result through `configSchema.safeParse()`.
4. `config` is a Proxy — validation runs the first time any field is read, not at module import. Tests call `resetConfigCache()` to re-run validation against new env.

Boolean variables accept `"true"` or `"false"` (case-insensitive). Comma-separated lists are trimmed and empty entries are dropped.

## Application

| Variable | Default | Process | Description |
|----------|---------|---------|-------------|
| `ENVIRONMENT` | `dev` | both | One of `dev`, `staging`, `prod`, `test`. Triggers production safety refinements (see Production Safety Refinements). |
| `TENANT` | `elastic-cloud` | both | Logical tenant; flows onto every normalized event. |

## Server (gateway only)

| Variable | Default | Process | Description |
|----------|---------|---------|-------------|
| `PORT` | `3000` | gateway | HTTP port for `Bun.serve()`. |

## Kafka

| Variable | Default | Process | Description |
|----------|---------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | both | Comma-separated bootstrap brokers. |
| `KAFKA_CLIENT_ID_GATEWAY` | `eventgate-gateway` | gateway | Client id for the producer. |
| `KAFKA_CLIENT_ID_WRITER` | `eventgate-writer` | writer | Client id for the consumer and DLQ producer. |
| `KAFKA_GROUP_ID` | `autoops-couchbase-writer-v1` | writer | Consumer group id on `events.v1`. |
| `KAFKA_AUTH` | `none` | both | `none` for local Redpanda, `iam` for AWS MSK Serverless. Required to be `iam` when `ENVIRONMENT=prod`. |
| `KAFKA_REGION` | unset | both | AWS region for MSK IAM SASL token signing. Required when `KAFKA_AUTH=iam`. |
| `KAFKA_TOPIC_RAW` | `ops.elastic.autoops.raw.v1` | gateway | Topic for verbatim webhook bodies. |
| `KAFKA_TOPIC_EVENTS` | `ops.elastic.autoops.events.v1` | both | Topic for normalized events. |
| `KAFKA_TOPIC_DLQ` | `ops.elastic.autoops.dlq.v1` | writer | DLQ topic for malformed messages. |

## Couchbase

| Variable | Default | Process | Description |
|----------|---------|---------|-------------|
| `COUCHBASE_ENABLED` | `true` | writer | When `false`, the writer logs events and skips Couchbase entirely. Used to deploy the writer before Capella is provisioned. |
| `COUCHBASE_CONNSTR` | `couchbase://localhost` | writer | Connection string. Must start with `couchbase://` or `couchbases://`. In `prod`, `couchbases://` (TLS) is required and `localhost` is rejected. |
| `COUCHBASE_USERNAME` | `Administrator` | writer | Cluster username. |
| `COUCHBASE_PASSWORD` | `password` | writer | Cluster password. The literal `"password"` is rejected when `ENVIRONMENT=prod`. |
| `COUCHBASE_BUCKET` | `ops` | writer | Bucket name. |
| `COUCHBASE_SCOPE` | `_default` | writer | Scope name. |
| `COUCHBASE_HISTORY_COLLECTION` | `autoops_events` | writer | Collection for append-only history docs. |
| `COUCHBASE_STATE_COLLECTION` | `autoops_state` | writer | Collection for rolling state docs. |

## Observability

| Variable | Default | Process | Description |
|----------|---------|---------|-------------|
| `LOG_LEVEL` | `info` | both | One of `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `silent`. Test suite forces `silent` via `test/preload.ts`. |

See [../operations/logging.md](../operations/logging.md) for output format and recommended levels per environment.

## Production Safety Refinements

When `ENVIRONMENT=prod`, `src/config/schemas.ts` runs additional checks via `.superRefine()`:

| Refinement | Reason |
|------------|--------|
| `kafka.auth` must be `iam` | MSK Serverless requires IAM SASL |
| `kafka.brokers` must not contain `localhost` | Catches stale env that would silently disconnect |
| `couchbase.connStr` must start with `couchbases://` | TLS-only against Capella |
| `couchbase.connStr` must not contain `localhost` | Catches stale env that would silently disconnect |
| `couchbase.password` must not be `"password"` | Default credential leak guard |

If `COUCHBASE_ENABLED=false`, the Couchbase refinements are skipped — the writer is allowed to deploy without a real cluster.

You can manually probe the refinements with:

```bash
ENVIRONMENT=prod COUCHBASE_CONNSTR=couchbase://localhost bun run start:gateway
```

Expected: exit with a Zod error citing `localhost` on `couchbase.connStr`.

## Local Development Defaults

`docker-compose.yml` advertises Redpanda on `localhost:9092` and Couchbase on `localhost:11210`/`8091`, which matches every default above. A minimal `.env` for local development:

```bash
# .env
ENVIRONMENT=dev
LOG_LEVEL=debug
```

`.env.example` in the repo root holds the full set of variables a deployment is likely to override. Treat `.env.example` as documentation only — do not commit real secrets to it.

## AWS Production Defaults

The ECS task definitions in `scripts/deploy/10-register-task-defs.sh` inject this environment block on both containers:

```bash
ENVIRONMENT=prod
KAFKA_AUTH=iam
KAFKA_REGION=<aws-region>
KAFKA_BROKERS=<msk-bootstrap-string>
COUCHBASE_ENABLED=false
LOG_LEVEL=info
```

When Couchbase Capella is provisioned and joined to the VPC, replace the writer's `COUCHBASE_ENABLED=false` with the Capella connection string, TLS-prefixed.

## See Also

- [architecture/overview.md](../architecture/overview.md) — how each setting maps to runtime behaviour.
- [deployment/aws-ecs.md](../deployment/aws-ecs.md) — where the production environment block is set.
- [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md) — project-agnostic pattern reference.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial environment variable reference created |
