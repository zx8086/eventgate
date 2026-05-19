# eventgate Documentation

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../guides/documentation-guide.md](../guides/documentation-guide.md)

Project-specific documentation for eventgate, a two-process Bun ingestion service for Elastic AutoOps webhooks. Use this index to find architecture, configuration, deployment, development, operations, security, and API references. Portable, project-agnostic patterns live in [`../guides/`](../guides/).

## Quick Navigation

| Need to... | Go to... |
|------------|----------|
| Run the service locally | [development/getting-started.md](development/getting-started.md) |
| Understand the two-process model and data flow | [architecture/overview.md](architecture/overview.md) |
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
| [overview.md](architecture/overview.md) | Two-process model, Kafka topics, Couchbase doc model, normalization contract |

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
| [aws-ecs.md](deployment/aws-ecs.md) | ECS Fargate deploy: one image, two task definitions, two services |
| [container-image.md](deployment/container-image.md) | Tiered Dockerfile, build arguments, healthcheck semantics |

### Development

| Document | Description |
|----------|-------------|
| [getting-started.md](development/getting-started.md) | Local dev loop with Redpanda and Couchbase via Docker Compose |

### Operations

| Document | Description |
|----------|-------------|
| [logging.md](operations/logging.md) | ECS-NDJSON output shape, `getLogger(component)` usage, log groups per process |

### Security

| Document | Description |
|----------|-------------|
| [container-scanning.md](security/container-scanning.md) | Trivy in CD, daily security audit workflow, severity policy |

### Plans

| Document | Description |
|----------|-------------|
| [v1-implementation-plan.md](plans/v1-implementation-plan.md) | Original v1 implementation plan, kept for historical reference |

---

## Related Documentation

| Document | Location |
|----------|----------|
| Project README | [../README.md](../README.md) |
| AI agent instructions | [../CLAUDE.md](../CLAUDE.md) |
| Portable programming guides | [../guides/](../guides/) |
| AWS deploy script runbook | [../scripts/deploy/README.md](../scripts/deploy/README.md) |

---

## Service Overview

### Key Capabilities

- Receives Elastic AutoOps webhook notifications over HTTP.
- Validates and normalizes payloads with a lenient Zod schema that tolerates hyphenated and camelCase keys, both `open`/`close` and `opened`/`closed` status spellings, and AutoOps' synthetic "Validate" body.
- Publishes raw and normalized events to Kafka for replay and downstream consumption.
- Projects normalized events into Couchbase as append-only history and rolling per-alert state.
- Sends parse failures and missing required fields to a DLQ topic instead of throwing.

### Technology Stack

| Layer | Choice |
|-------|--------|
| Runtime | Bun 1.3.11+ |
| Language | TypeScript 5.x (strict mode) |
| HTTP server | `Bun.serve()` with object-style routes |
| Kafka client | `@platformatic/kafka` |
| Database | Couchbase (Capella in production, Server 7.6 locally) |
| Validation | Zod v4 with `.strictObject()` and `.superRefine()` |
| Logging | Pino 10 with `@elastic/ecs-pino-format`, synchronous destination |
| Container | Tiered Dockerfile (distroless Tier 2 default, Alpine Tier 1 fallback) |
| Orchestrator | AWS ECS Fargate with one image and two task definitions |

### Topics and Collections

| Resource | Purpose |
|----------|---------|
| `ops.elastic.autoops.raw.v1` | Verbatim webhook body for replay |
| `ops.elastic.autoops.events.v1` | Normalized events; consumed by the writer |
| `ops.elastic.autoops.dlq.v1` | Malformed messages quarantined by the writer |
| `autoops_events` collection | Append-only history, one document per delivery |
| `autoops_state` collection | Rolling per-alert state, one document per alert signature |

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial documentation index created |
