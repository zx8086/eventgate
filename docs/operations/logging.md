# Logging

> **Targets:** Bun 1.3.11+ | TypeScript 5.x | Pino 10
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate emits structured logs in ECS NDJSON via Pino 10 with `@elastic/ecs-pino-format`. Both processes share one logger factory in `src/logging/index.ts`; each call site obtains a child logger with a `component` binding so the gateway and writer streams can be filtered without the noise of separate log groups merging. This document describes the output shape, how to call the logger, and how the two processes are separated at the CloudWatch level.

## Output Shape

Each log line is a single JSON object on its own line:

```json
{"@timestamp":"2026-05-19T10:14:22.108Z","log.level":"info","service.name":"eventgate","service.version":"0.1.0","service.environment":"prod","ecs.version":"8.10.0","component":"gateway","host":"0.0.0.0","port":3000,"topics":{"raw":"ops.elastic.autoops.raw.v1","events":"ops.elastic.autoops.events.v1"},"message":"gateway listening"}
```

| Field | Source | Notes |
|-------|--------|-------|
| `@timestamp` | `@elastic/ecs-pino-format` | ISO-8601 UTC |
| `log.level` | Pino level | `trace` ... `fatal` |
| `service.name` | `config.app.name` from `package.json` | Same for both processes (`eventgate`) |
| `service.version` | `config.app.version` from `package.json` | |
| `service.environment` | `config.app.environment` | `dev`, `staging`, `prod`, `test` |
| `ecs.version` | `@elastic/ecs-pino-format` | |
| `component` | `getLogger(component)` call site | Differentiates `gateway`, `gateway.routes`, `writer`, etc. |
| `message` | Second argument to logger call | |

`service.name` is identical for both processes — `component` plus the CloudWatch log group name is how you tell them apart.

## Calling the Logger

Always use `getLogger(component)` from `src/logging/index.ts`. Never call `console.log` or `console.error` in `src/`.

```typescript
// Correct -- module-level logger with a stable component binding
import { getLogger } from "../logging/index.ts";

const log = getLogger("gateway.routes");

log.info({ method, path, ua: req.headers.get("user-agent") }, "unmatched request");
log.error({ err, resourceId }, "kafka publish failed");
```

Pino convention applies: the first argument is the bindings object, the second is the message. Errors go on the `err` key — the `@elastic/ecs-pino-format` serializer flattens them into ECS-compatible fields.

```typescript
// Incorrect -- err shadowed by template literal, no structured fields
console.error(`kafka publish failed for ${resourceId}: ${err.message}`);

// Correct -- structured, queryable
log.error({ err, resourceId }, "kafka publish failed");
```

For project-agnostic logger usage patterns see [../../guides/bun-logging-guide.md](../../guides/bun-logging-guide.md).

## Synchronous Destination (Bun Requirement)

`src/logging/index.ts` builds the root logger with a synchronous destination:

```typescript
const destination = { write: (s: string) => process.stdout.write(s) };
```

`pino.transport()` is **never** used. Pino transports rely on Node worker threads, which are not compatible with Bun. The synchronous destination guarantees every log line is flushed before the process exits — important for SIGTERM handling, where the shutdown path logs and then `process.exit(0)`.

If a future change introduces async log shipping, do it as a sidecar log forwarder consuming stdout, not as a Pino transport.

## Log Level

`LOG_LEVEL` (default `info`) controls the threshold. Valid values: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `silent`.

| Environment | Recommended level | Reason |
|-------------|------------------|--------|
| Local dev | `debug` | See routing decisions, schema parses, consumer offsets |
| `test` | `silent` | Forced by `test/preload.ts` so `bun test` output is clean |
| `staging` | `info` | Same shape as prod, easier to tail |
| `prod` | `info` | Avoid log-storm cost on CloudWatch; raise to `debug` per-task only for incidents |

To raise the level on a single ECS task for an incident, register a new task definition revision with `LOG_LEVEL=debug` and update the affected service — do not edit a running task in place.

## CloudWatch Log Groups

The two ECS task definitions write to separate log groups:

| Process | Log group | Stream prefix |
|---------|-----------|---------------|
| gateway | `/eventgate/gateway` | `ecs` |
| writer | `/eventgate/writer` | `ecs` |

The split is what makes `service.name=eventgate` on both lines tolerable — filter by log group first, then by `component`. The log groups are created by `scripts/deploy/06-log-groups.sh`.

## Useful Filter Patterns

CloudWatch Logs Insights queries:

```text
# all webhook receipts in the last hour
fields @timestamp, component, resourceId, idempotencyKey
| filter component = "gateway.routes"
| filter message = "autoops.event.received"
| sort @timestamp desc

# DLQ activity in the writer
fields @timestamp, reason, resourceId
| filter message like /DLQ/
| sort @timestamp desc

# kafka publish failures (gateway-side)
fields @timestamp, err.message, resourceId
| filter message = "kafka publish failed"
| sort @timestamp desc
```

## What to Log and What Not To

| Log it | Do not log it |
|--------|---------------|
| `{ err }` on every caught exception | The full webhook body at `info` (PII risk; AutoOps payloads may include node names and indices that customers consider sensitive) |
| `resourceId`, `alertSignature`, `idempotencyKey`, `eventType`, `severity` | Couchbase or MSK credentials, even on auth failures |
| Consumer offsets and partition assignments at `debug` | Verbatim raw Kafka payloads at `info` — they are already on the `raw.v1` topic for replay |

The gateway currently logs the parsed body at `warn` when schema validation fails (`src/gateway/routes.ts:40-43`). That is acceptable because validation failures are rare and the operator needs the payload to fix the connector template — but reconsider this if the volume grows.

## Shutdown Logging

Both processes register SIGINT and SIGTERM handlers that log the signal, then call `process.exit(0)` after the producer / consumer / Couchbase client have closed. The synchronous destination guarantees the final `shutting down` line is flushed before the process exits — see `src/gateway/index.ts:37-46` and `src/writer/index.ts:98-111`.

## See Also

- [../architecture/overview.md](../architecture/overview.md) — what each process does (so you know which `component` to filter on).
- [../deployment/aws-ecs.md](../deployment/aws-ecs.md) — where the log groups are configured.
- [../../guides/bun-logging-guide.md](../../guides/bun-logging-guide.md) — project-agnostic logger patterns and Bun-specific gotchas.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial logging doc created |
