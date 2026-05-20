# Webhooks API

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-20
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

The gateway exposes two HTTP endpoints — the Elastic AutoOps webhook receiver and a healthcheck. This document is the contract: request and response shapes, status codes, and the opportunistic idempotency-key header the gateway stamps when it recognizes an AutoOps body. The implementation lives in `src/gateway/routes.ts` and `src/gateway/idempotencyKey.ts`.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/webhooks/elastic/autoops` | Receive an AutoOps webhook delivery |
| `GET` | `/healthz` | Service liveness, gated on Kafka producer connectivity |

Any other path returns `404` with the body `Not found` and is logged at `warn` with `method`, `path`, and `user-agent` bindings.

## POST /webhooks/elastic/autoops

### Request

| Header | Required | Value |
|--------|----------|-------|
| `Content-Type` | yes | `application/json` |

### Request body

The gateway is **accept-everything**: any valid JSON body is accepted. There is no schema validation and no normalization. The body is forwarded verbatim to the route's configured Kafka topic (the seed `elastic-autoops` route publishes to `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`). Downstream consumer services (not part of this repo) are responsible for parsing, validating, and normalizing the payload.

The only fields the gateway inspects are used to derive a message key and an optional `idempotencyKey` header:

| Field | Used for | Notes |
|-------|----------|-------|
| `resourceId` or `deployment-id` | Kafka message key | First non-empty string wins; falls back to the literal `unkeyed` |
| `title`, `status`, `startTime` / `start-time`, `endTime` / `end-time` | Opportunistic `idempotencyKey` header | All four must be present strings for the header to be stamped |

The body itself is not inspected beyond those keys.

### Responses

| Status | Condition | Body |
|--------|-----------|------|
| `202 Accepted` | JSON parsed, outbox enqueue (or inline publish) attempted | `{ "accepted": true }` |
| `400 Bad Request` | JSON parse failure | `{ "accepted": false, "error": "invalid JSON body" }` |
| `500 Internal Server Error` | Outbox enqueue failed (durability layer broken) | `{ "accepted": false, "error": "outbox enqueue failed" }` |

When the outbox is disabled (`OUTBOX_ENABLED=false`), inline Kafka publish failures are logged at `warn` but the gateway still returns `202`. This is the **receive-fast, process-async** pattern: AutoOps gets a fast ACK and the message can be replayed later from upstream if necessary. When the outbox is enabled (the default), durability is owned by SQLite and the drainer — see [../architecture/outbox.md](../architecture/outbox.md).

### Example success

```bash
curl -i -X POST http://localhost:3000/webhooks/elastic/autoops \
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

```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{"accepted":true}
```

### Example invalid JSON

```bash
curl -i -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d 'not-json'
```

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"accepted":false,"error":"invalid JSON body"}
```

## GET /healthz

### Request

No body, no headers required.

### Responses

| Status | Condition | Body |
|--------|-----------|------|
| `200 OK` | Kafka producer is connected | `{ "ok": true, "producer": { "connected": true }, "outbox": { ... } }` |
| `503 Service Unavailable` | Kafka producer is not connected | `{ "ok": false, "producer": { "connected": false }, "outbox": { ... } }` |

The `outbox` block reports `{ enabled: true, pending, failed, oldestPendingAgeMs }` when the outbox is on, or `{ enabled: false }` otherwise. The ALB target-group healthcheck and the Dockerfile `HEALTHCHECK` both target this path. The healthcheck only probes the gateway's own producer state; it does not probe any downstream system.

## Idempotency Header

When the gateway can extract `resourceId`/`deployment-id`, `title`, and `status` from the body, it stamps a deterministic `idempotencyKey` Kafka header on the outgoing `raw.v1` message:

```
idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)
```

The key is not returned in the HTTP response — operators read it off the Kafka header. Downstream consumers may dedupe on `idempotencyKey` if their write path is not naturally idempotent. The header is **opportunistic**: bodies that don't expose the four required fields ship with no header, and the gateway still returns `202`.

## Authentication

There is no authentication on the webhook endpoint in v1. AutoOps webhook connectors expose only Name, URL, Method, custom Headers, and Body — no native HMAC, bearer, or basic-auth. Operators should put the gateway behind ALB / Cloudflare Access or an equivalent network control until v2 adds shared-token header validation.

## Logging

The gateway logs at `warn` if the outbox enqueue fails (`outbox enqueue failed`) and at `warn` if an inline (outbox-disabled) Kafka publish fails (`kafka publish failed; will not retry without outbox`). Successful deliveries do not log per-request — the outbox row and the resulting Kafka message are the audit trail. The `component` binding is `gateway.routes` — see [../operations/logging.md](../operations/logging.md) for filter patterns.

## See Also

- [../architecture/overview.md](../architecture/overview.md) — accept-everything flow and the role of the outbox.
- [../architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md) — which Kafka backend the producer is connected to.
- [../architecture/outbox.md](../architecture/outbox.md) — durability layer between the HTTP handler and Kafka.
- [../development/getting-started.md](../development/getting-started.md) — local curl + smoke test.
- [../operations/logging.md](../operations/logging.md) — how to find webhook deliveries in CloudWatch.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial webhooks API doc created |
| 2026-05-19 | Removed writer/Couchbase references from the idempotency section (SIO-795) |
| 2026-05-19 | Rewritten for accept-everything contract: no schema validation, single 202 body, `idempotencyKey` moved to Kafka header (SIO-801) |
