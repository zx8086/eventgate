# Kafka Provider Factory (eventgate)

> **Source of truth for the pattern:** [`../../guides/kafka-provider-factory.md`](../../guides/kafka-provider-factory.md). This document is the eventgate-specific application of that pattern.

eventgate is gateway-only: HTTP in, Kafka out. The factory lets a single image target local Redpanda for development, AWS MSK for production, or Confluent Cloud — selected entirely by environment variables. No code change required to swap providers.

## File layout

```
src/kafka/
  producer.ts                 EventProducer wrapper around @platformatic/kafka Producer
  providers/
    types.ts                  KafkaProvider, KafkaConnectionConfig, MskAuthMode
    errors.ts                 KafkaProviderError + ProviderErrorCode
    local.ts                  LocalKafkaProvider — PLAINTEXT, no SASL/TLS
    confluent.ts              ConfluentKafkaProvider — SASL/PLAIN + TLS
    msk.ts                    MskKafkaProvider — SASL/OAUTHBEARER + TLS, broker discovery
    index.ts                  createKafkaProvider(config) factory + re-exports
```

The factory is wired at startup in `src/gateway/index.ts`:

```ts
const provider = createKafkaProvider(config);
log.info({ provider: provider.name, providerType: provider.type }, "kafka provider selected");
const producer = await createProducer(config.kafka.clientId, provider);
```

`provider.close()` runs on SIGINT/SIGTERM alongside `producer.disconnect()` — important for MSK to drop the cached IAM token.

## Provider selection

`KAFKA_PROVIDER` selects the backend. The config Zod schema (`src/config/schemas.ts`) enforces the per-provider field requirements via `superRefine`:

| Provider | Required env vars | Optional |
|----------|-------------------|----------|
| `local` (default) | — | `KAFKA_LOCAL_BOOTSTRAP_SERVERS` (default `localhost:9092`) |
| `msk` | `MSK_REGION` plus one of `MSK_CLUSTER_ARN` / `MSK_BROKERS` | `MSK_AUTH_MODE` (default `iam`) |
| `confluent` | `CONFLUENT_BOOTSTRAP_SERVERS`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET` | — |

Two extra rules:

- `KAFKA_PROVIDER=local` is **rejected** when `ENVIRONMENT=prod`. Prod must use `msk` or `confluent`.
- `MSK_AUTH_MODE` defaults to `iam`. The provider logs a warning at startup if `MSK_AUTH_MODE` is unset, so operators see the resolved value in logs.

## Environment variables

| Variable | Used when | Purpose |
|----------|-----------|---------|
| `KAFKA_PROVIDER` | all | `local` \| `msk` \| `confluent`. Default `local`. |
| `KAFKA_CLIENT_ID` | all | Producer client id. Default `eventgate-gateway`. |
| `KAFKA_TOPIC_RAW` | all | Raw webhook topic. |
| `KAFKA_TOPIC_EVENTS` | all | Normalized events topic. |
| `KAFKA_TOPIC_DLQ` | all | DLQ topic. |
| `KAFKA_LOCAL_BOOTSTRAP_SERVERS` | `local` | CSV bootstrap brokers. Default `localhost:9092`. |
| `MSK_REGION` | `msk` | AWS region of the MSK cluster. |
| `MSK_CLUSTER_ARN` | `msk` | Cluster ARN; brokers discovered via `GetBootstrapBrokersCommand`. |
| `MSK_BROKERS` | `msk` | Skip discovery; CSV bootstrap brokers. One of `MSK_BROKERS` / `MSK_CLUSTER_ARN` required. |
| `MSK_AUTH_MODE` | `msk` | `iam` (default) \| `tls` \| `none`. |
| `CONFLUENT_BOOTSTRAP_SERVERS` | `confluent` | Bootstrap servers (host:port[,host:port]). |
| `CONFLUENT_API_KEY` | `confluent` | SASL/PLAIN username. |
| `CONFLUENT_API_SECRET` | `confluent` | SASL/PLAIN password. |

## eventgate-specific notes

- **Client library:** `@platformatic/kafka` 2.x. The `KafkaConnectionConfig` field names (`bootstrapBrokers`, `tls`, `sasl: { mechanism, username, password, token }`) line up with `@platformatic/kafka`'s `ConnectionOptions` 1:1 — no adapter layer needed in `src/kafka/producer.ts`.
- **Producer only.** eventgate does not consume from Kafka in this repo; downstream consumers live in other services. The factory therefore exposes only `getConnectionConfig()`. The portable guide includes an optional `getClusterMetadata()` for ops tooling, but it is not implemented here.
- **No Confluent REST client.** v1 of this factory in eventgate only needs producer credentials. If a future health probe needs cluster metadata, add `ConfluentRestClient` per the portable guide.
- **AWS SDK install cost.** `@aws-sdk/client-kafka` and `aws-msk-iam-sasl-signer-js` are listed as regular `dependencies` in `package.json` but are **lazy-imported** inside `src/kafka/providers/msk.ts`. Local and Confluent runs never load them at runtime; install cost (~10-15 MB) is the only price for projects that never use MSK.
- **Token caching.** The MSK provider caches the IAM token with a 60s safety margin before `expiryTime`. Tests in `test/unit/kafka.providers.msk.test.ts` exercise this directly. Tokens are dropped on `provider.close()` (SIGTERM/SIGINT).

## Verification

```bash
# Local
KAFKA_PROVIDER=local bun run dev:gateway

# MSK (prod-shaped)
ENVIRONMENT=prod KAFKA_PROVIDER=msk MSK_REGION=eu-central-1 \
  MSK_CLUSTER_ARN=arn:aws:kafka:eu-central-1:123:cluster/x/u bun run start:gateway

# Confluent (prod-shaped)
ENVIRONMENT=prod KAFKA_PROVIDER=confluent \
  CONFLUENT_BOOTSTRAP_SERVERS=pkc-1.eu-central-1.aws.confluent.cloud:9092 \
  CONFLUENT_API_KEY=k CONFLUENT_API_SECRET=s bun run start:gateway

# Prod-safety: must fail
ENVIRONMENT=prod KAFKA_PROVIDER=local bun run start:gateway
```

The gateway logs `kafka provider selected` with `provider.name` and `provider.type` on startup. Use that log line to confirm which provider connected in any environment.
