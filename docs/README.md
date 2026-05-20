# eventgate Documentation

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-20
> **Conventions:** See [../guides/documentation-guide.md](../guides/documentation-guide.md)

Project-specific documentation for eventgate, a single-process Bun ingestion service for webhook sources (Elastic AutoOps and any other vendor declared in `config.routes[]`). HTTP in, Kafka out. Use this index to find architecture, configuration, deployment, development, operations, security, and API references. Portable, project-agnostic patterns live in [`../guides/`](../guides/).

## Quick Navigation

| Need to... | Go to... |
|------------|----------|
| Run the service locally | [development/getting-started.md](development/getting-started.md) |
| Understand the gateway architecture | [architecture/overview.md](architecture/overview.md) |
| Trace a webhook or admin request end-to-end | [architecture/request-flows.md](architecture/request-flows.md) |
| Switch Kafka backend (local / MSK / Confluent) | [architecture/kafka-provider-factory.md](architecture/kafka-provider-factory.md) |
| Understand the SQLite outbox / Kafka durability | [architecture/outbox.md](architecture/outbox.md) |
| Look up an environment variable | [configuration/environment-variables.md](configuration/environment-variables.md) |
| Deploy to AWS ECS Fargate | [deployment/aws-ecs.md](deployment/aws-ecs.md) |
| Copy a task definition with EBS outbox volume | [deployment/task-definition-example.md](deployment/task-definition-example.md) |
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
| [overview.md](architecture/overview.md) | System diagram, layers, topic naming policy, reserved paths, admin endpoint |
| [request-flows.md](architecture/request-flows.md) | Sequence diagrams: webhook ingest, drainer loop, admin reload |
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
| [task-definition-example.md](deployment/task-definition-example.md) | Copy-pasteable Fargate task definition + service JSON, CloudFormation, and Terraform with EBS outbox volume |
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

- Receives JSON webhooks over HTTP on every path declared in `config.routes[]`. Multi-source today (Elastic AutoOps shipped; new vendors are config-only).
- Accepts any valid JSON body — non-JSON bodies return 400, everything else returns 202. No Zod validation or normalization at the gateway (webhook bodies are accepted as-is; config IS Zod-validated).
- Writes one row per request to the route's `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` topic via the SQLite outbox for replay; an opportunistic `idempotencyKey` is attached as a Kafka header when the route's idempotency strategy applies.
- Optional `PUT /admin/routes` admin endpoint (token-protected, hot-reload via `server.reload`, persisted to `ROUTES_FILE`) for runtime route changes without redeploy.
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

Topics are declared per route in `config.routes[]` and validated at startup against the org-wide naming policy. The gateway is the source connector in that taxonomy.

| Pattern | Use | Example |
|---|---|---|
| `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` | Gateway-owned. Verbatim webhook body. | `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` |
| `DLQ_T_<topic>` | Optional companion DLQ name. Declared on the route; gateway never publishes here. | `DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` |

Forbidden as gateway topics (each rejected at startup with a distinct message): `T_PUBLIC_*`, `T_PRIVATE_SINK_*`, `T_PRIVATE_*_RICH_NOTIFICATIONS`, `T_PRIVATE_*_EVENTS`, `DLQ_T_*` (as the primary topic), and Kafka/Confluent system prefixes. See [architecture/overview.md](architecture/overview.md) for the full policy.

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial documentation index created |
| 2026-05-19 | Refactored to gateway-only architecture with Kafka provider factory (SIO-795) |
| 2026-05-19 | Added SQLite outbox for gateway durability against Kafka outages (SIO-799) |
| 2026-05-20 | Updated for multi-route config + admin endpoint + topic naming policy + request-flows companion doc (SIO-802, SIO-803, SIO-804) |
