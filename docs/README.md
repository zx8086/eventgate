# eventgate Documentation

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../guides/documentation-guide.md](../guides/documentation-guide.md)

Project-specific documentation for eventgate, a single-process Bun ingestion service for Elastic AutoOps webhooks. HTTP in, Kafka out. Use this index to find architecture, configuration, deployment, development, operations, security, and API references. Portable, project-agnostic patterns live in [`../guides/`](../guides/).

## Quick Navigation

| Need to... | Go to... |
|------------|----------|
| Run the service locally | [development/getting-started.md](development/getting-started.md) |
| Understand the gateway architecture | [architecture/overview.md](architecture/overview.md) |
| Switch Kafka backend (local / MSK / Confluent) | [architecture/kafka-provider-factory.md](architecture/kafka-provider-factory.md) |
| Understand the SQLite outbox / Kafka durability | [architecture/outbox.md](architecture/outbox.md) |
| Look up an environment variable | [configuration/environment-variables.md](configuration/environment-variables.md) |
| Deploy to AWS ECS Fargate | [deployment/aws-ecs.md](deployment/aws-ecs.md) |
| Understand the container image (Tier 1 / Tier 2) | [deployment/container-image.md](deployment/container-image.md) |
| Read log output or change log level | [operations/logging.md](operations/logging.md) |
| Call the webhook or healthcheck endpoint | [api/webhooks.md](api/webhooks.md) |
| Investigate a container vulnerability finding | [security/container-scanning.md](security/container-scanning.md) |
| Read the original v1 implementation plan | [plans/v1-implementation-plan.md](plans/v1-implementation-plan.md) |

---

## By Category

### Architecture

| Document | Description |
|----------|-------------|
| [overview.md](architecture/overview.md) | Gateway architecture, Kafka topics, accept-everything contract |
| [kafka-provider-factory.md](architecture/kafka-provider-factory.md) | Provider abstraction for local / MSK / Confluent backends |
| [outbox.md](architecture/outbox.md) | SQLite outbox + drainer for durability against Kafka outages |

### API

| Document | Description |
|----------|-------------|
| [webhooks.md](api/webhooks.md) | `POST /webhooks/elastic/autoops` and `GET /healthz` reference |

### Configuration

| Document | Description |
|----------|-------------|
| [environment-variables.md](configuration/environment-variables.md) | Every environment variable the 4-pillar config reads |

### Deployment

| Document | Description |
|----------|-------------|
| [aws-ecs.md](deployment/aws-ecs.md) | ECS Fargate deploy: one image, one task definition, one service |
| [container-image.md](deployment/container-image.md) | Tiered Dockerfile, build arguments, healthcheck semantics |

### Development

| Document | Description |
|----------|-------------|
| [getting-started.md](development/getting-started.md) | Local dev loop with Redpanda via Docker Compose |

### Operations

| Document | Description |
|----------|-------------|
| [logging.md](operations/logging.md) | ECS-NDJSON output shape, `getLogger(component)` usage, log group |

### Security

| Document | Description |
|----------|-------------|
| [container-scanning.md](security/container-scanning.md) | Trivy in CD, daily security audit workflow, severity policy |

### Plans

| Document | Description |
|----------|-------------|
| [v1-implementation-plan.md](plans/v1-implementation-plan.md) | Original v1 implementation plan, kept for historical reference (predates Couchbase removal) |

---

## Related Documentation

| Document | Location |
|----------|----------|
| Project README | [../README.md](../README.md) |
| AI agent instructions | [../CLAUDE.md](../CLAUDE.md) |
| Portable programming guides | [../guides/](../guides/) |
| Portable Kafka provider factory pattern | [../guides/kafka-provider-factory.md](../guides/kafka-provider-factory.md) |
| AWS deploy script runbook | [../scripts/deploy/README.md](../scripts/deploy/README.md) |

---

## Service Overview

### Key Capabilities

- Receives Elastic AutoOps webhook notifications over HTTP.
- Accepts any valid JSON body — non-JSON bodies return 400, everything else returns 202. No Zod validation or normalization at the gateway.
- Writes one row per request to `ops.elastic.autoops.raw.v1` via the SQLite outbox for replay; an opportunistic `idempotencyKey` is attached as a Kafka header when it can be derived from the body.
- Connects to local Redpanda, AWS MSK, or Confluent Cloud via a provider factory — selected entirely by environment variables.

### Technology Stack

| Layer | Choice |
|-------|--------|
| Runtime | Bun 1.3.11+ |
| Language | TypeScript 5.x (strict mode) |
| HTTP server | `Bun.serve()` with object-style routes |
| Kafka client | `@platformatic/kafka` |
| Kafka backends | Local Redpanda \| AWS MSK (IAM/TLS/none) \| Confluent Cloud (SASL/PLAIN + TLS) |
| Validation | Zod v4 with `.strictObject()` and `.superRefine()` (config only; webhook body is accepted as-is) |
| Logging | Pino 10 with `@elastic/ecs-pino-format`, synchronous destination |
| Container | Tiered Dockerfile (distroless Tier 2 default, Alpine Tier 1 fallback) |
| Orchestrator | AWS ECS Fargate, one image and one task definition |

### Topics

| Resource | Purpose |
|----------|---------|
| `ops.elastic.autoops.raw.v1` | Verbatim webhook body for replay — the only topic the gateway writes to |
| `ops.elastic.autoops.events.v1` | Reserved for future normalization consumers; gateway does not publish here |
| `ops.elastic.autoops.dlq.v1` | Reserved for future downstream consumers; gateway does not publish here |

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial documentation index created |
| 2026-05-19 | Refactored to gateway-only architecture with Kafka provider factory (SIO-795) |
| 2026-05-19 | Added SQLite outbox for gateway durability against Kafka outages (SIO-799) |
