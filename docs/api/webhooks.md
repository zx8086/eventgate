# Webhooks API

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

The gateway exposes two HTTP endpoints — the Elastic AutoOps webhook receiver and a healthcheck. This document is the contract: request and response shapes, status codes, idempotency semantics, and the synthetic-test path used by AutoOps' "Validate" button. The implementation lives in `src/gateway/routes.ts`, `src/gateway/schema.ts`, and `src/normalize.ts`.

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

The schema is lenient on shape and strict on the two identifiers required to compute the alert signature and idempotency key. It accepts both AutoOps' hyphenated default keys (`deployment-id`, `start-time`, ...) and the camelCase form documented in the project README. Unsubstituted `${VAR}` placeholders are stripped before validation (see Synthetic Validation Body).

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `resourceId` | string | yes | Elastic deployment / resource id (`RESOURCE_ID`) |
| `title` | string | yes | Alert title (`TITLE`); part of `alertSignature` |
| `resourceName` | string | defaults to `""` | Human-readable resource name (`RESOURCE_NAME`) |
| `status` | string | defaults to `"unknown"` | `open` / `close`; `opened` / `closed` / `resolved` also tolerated |
| `severity` | string | optional | `High` / `Medium` / `Low`; case-insensitive |
| `description` | string | optional | (`DESCRIPTION`) |
| `message` | string | optional | (`MESSAGE`) |
| `startTime` | string | optional | ISO-8601 (`START_TIME`) |
| `endTime` | string \| null | optional | ISO-8601 (`END_TIME`) |
| `endpointType` | string | optional | (`ENDPOINT_TYPE`) |
| `affectedNodes` | string \| string[] | optional | Comma-separated or array (`AFFECTED_NODES`) |
| `affectedIndices` | string \| string[] | optional | Comma-separated or array (`AFFECTED_INDICES`) |
| `eventLink` | string | optional | Deep-link to AutoOps (`EVENT_LINK`) |
| `source` | string | optional | Upstream source tag |

The schema is `.loose()` — unknown keys are kept but ignored.

### Recommended AutoOps connector body

In the AutoOps connector body template, map the AutoOps variables to camelCase:

```json
{
  "resourceId": "RESOURCE_ID",
  "resourceName": "RESOURCE_NAME",
  "title": "TITLE",
  "description": "DESCRIPTION",
  "severity": "SEVERITY",
  "status": "STATUS",
  "message": "MESSAGE",
  "startTime": "START_TIME",
  "endTime": "END_TIME",
  "endpointType": "ENDPOINT_TYPE",
  "affectedNodes": "AFFECTED_NODES",
  "affectedIndices": "AFFECTED_INDICES",
  "eventLink": "EVENT_LINK"
}
```

The hyphenated-key form (`deployment-id`, `start-time`, etc.) is also accepted and is normalized to camelCase before validation in `src/gateway/schema.ts`.

### Responses

| Status | Condition | Body |
|--------|-----------|------|
| `202 Accepted` | Valid payload, normalized, publish attempted | `{ "accepted": true, "resourceId": "...", "idempotencyKey": "..." }` |
| `202 Accepted` | Synthetic validation body (every field is a `${VAR}` placeholder) | `{ "accepted": true, "validation": "synthetic" }` |
| `400 Bad Request` | JSON parse failure | `{ "accepted": false, "error": "invalid JSON body" }` |
| `400 Bad Request` | Schema validation failure | `{ "accepted": false, "error": "schema validation failed", "issues": [ ... Zod issues ... ] }` |

The gateway returns `202` even if the Kafka publish fails — the failure is logged at `warn` with `{ err, resourceId }` but does not block the response. This is the **receive-fast, process-async** pattern: AutoOps gets a fast ACK and the message can be replayed later from upstream if necessary.

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

{"accepted":true,"resourceId":"r-123","idempotencyKey":"3a9e..."}
```

### Example schema failure

```bash
curl -i -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d '{"title": "no resourceId here"}'
```

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"accepted":false,"error":"schema validation failed","issues":[{"path":["resourceId"],"message":"Required",...}]}
```

## GET /healthz

### Request

No body, no headers required.

### Responses

| Status | Condition | Body |
|--------|-----------|------|
| `200 OK` | Kafka producer is connected | `{ "ok": true }` |
| `503 Service Unavailable` | Kafka producer is not connected | `{ "ok": false }` |

The ALB target-group healthcheck and the Dockerfile `HEALTHCHECK` both target this path. The healthcheck deliberately does not probe Couchbase — the gateway does not call Couchbase, so it would be the wrong dependency to gate on.

## Idempotency Contract

The gateway computes two identifiers from each accepted payload:

| Identifier | Formula | Used as |
|-----------|---------|---------|
| `alertSignature` | `slugify(resourceId :: title)` | Part of the rolling state document key in Couchbase. Stable across the open + close pair of the same alert. |
| `idempotencyKey` | `sha256(resourceId :: title :: status :: startTime :: endTime)` | Returned in the `202` response body. Part of the history document key in Couchbase. Stable across retries of the same delivery; differs between the open and close of the same alert because `status` differs. |

This means:

- **Retries of the same delivery** (e.g. AutoOps marks the first send as failed and resends) write the same history document on the writer side and increment the state document only on the second, third, ... delivery if you do not guard on `idempotencyKey`. The writer currently increments unconditionally — see [../architecture/overview.md#idempotency-guarantee](../architecture/overview.md#idempotency-guarantee).
- **An open + close pair** of the same alert produces two history documents (different `idempotencyKey`) and one state document (same `alertSignature`).

## Synthetic Validation Body

The AutoOps "Validate" button in the connector UI posts the body template **without substituting placeholders** — every value comes through as the literal string `${RESOURCE_ID}`, `${TITLE}`, etc. `isSyntheticAutoOpsTest()` in `src/gateway/schema.ts` detects this case (every value is a string matching `${...}`) and the route returns `202 { "validation": "synthetic" }` instead of failing schema validation. Real malformed deliveries still get `400` because they will have at least one non-placeholder field.

## Authentication

There is no authentication on the webhook endpoint in v1. AutoOps webhook connectors expose only Name, URL, Method, custom Headers, and Body — no native HMAC, bearer, or basic-auth. Operators should put the gateway behind ALB / Cloudflare Access or an equivalent network control until v2 adds shared-token header validation.

## Logging

Each accepted webhook delivery logs one `autoops.event.received` line at `info` with the normalized event as a binding. Schema failures log at `warn` with the Zod issues and the original body. Kafka publish failures log at `warn` with `{ err, resourceId }`. The `component` binding is `gateway.routes` — see [../operations/logging.md](../operations/logging.md) for filter patterns.

## See Also

- [../architecture/overview.md](../architecture/overview.md) — the normalization contract that defines `alertSignature` and `idempotencyKey`.
- [../development/getting-started.md](../development/getting-started.md) — local curl + smoke test.
- [../operations/logging.md](../operations/logging.md) — how to find webhook deliveries in CloudWatch.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial webhooks API doc created |
