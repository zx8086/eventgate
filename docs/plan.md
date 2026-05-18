# Plan: Bun-based Elastic AutoOps webhook ingestion service (`eventgate`)

## Context

The repo `zx8086/eventgate` is currently empty (only a README on branch `claude/bun-autoops-webhooks-Ov8jw`). The goal is to stand up a v1 Bun service that receives Elastic AutoOps webhook notifications, durably publishes them to Kafka, and persists them to Couchbase via a separate consumer. The architecture follows the "receive fast, process async" webhook pattern: Bun does minimal validation + normalization, publishes to Kafka, then ACKs 202. A separate Kafka consumer projects history (append-only) and current-state documents into Couchbase. This gives a resilient ingestion path with replay, while Couchbase becomes the source for operational queries and trend analysis. Flink is explicitly out of scope for v1.

Grounded in the actual Elastic AutoOps docs (Notifications Settings + FAQ):
- The webhook connector exposes only **Name, URL, Method (POST), custom Headers, Body** — there is **no native HMAC, bearer, or basic-auth** option. Any auth has to ride on a custom header that the operator configures by hand. v1 ships without auth (env-var-gated allowlist deferred to v2).
- Variable set is fixed: `RESOURCE_ID, RESOURCE_NAME, TITLE, DESCRIPTION, SEVERITY, STATUS, MESSAGE, START_TIME, END_TIME, ENDPOINT_TYPE, AFFECTED_NODES, AFFECTED_INDICES, EVENT_LINK`.
- `SEVERITY` ∈ `High|Medium|Low`; `STATUS` ∈ `open|close` (the docs say "open or close", not "opened/closed" — normalization must handle both spellings since the operator controls the body template).
- AutoOps has a built-in notification "Delay" filter that suppresses notifications when events close inside the window, so we get some dedup upstream — but we still compute our own idempotency key for safety.
- No documented delivery guarantees or retry semantics for the webhook connector — the notifications report shows "Notification failed to send" as a terminal state. Our consumer must therefore treat duplicates as possible (idempotent upserts) and we keep a DLQ topic for downstream failures.

## Architecture

```
Elastic AutoOps  --POST-->  Bun gateway  --produce-->  Kafka  --consume-->  Couchbase
                            (gateway.ts)               topics              (writer.ts)
                                |                       - raw.v1
                                |                       - events.v1
                                +-- 202 ack             - dlq.v1
```

Two long-running Bun processes in one repo:
- **gateway** (`src/gateway/index.ts`) — HTTP receiver, publishes raw + normalized events to Kafka.
- **writer** (`src/writer/index.ts`) — Kafka consumer, writes history + state docs to Couchbase, sends failures to DLQ.

## Repository layout

```
eventgate/
├── package.json
├── tsconfig.json
├── bunfig.toml
├── .gitignore
├── .env.example
├── docker-compose.yml          # local Redpanda (Kafka API) + Couchbase
├── README.md                   # extend existing
└── src/
    ├── types.ts                # shared types: ElasticAutoOpsWebhook, NormalizedEvent, AutoOpsEventHistoryDoc, AutoOpsStateDoc
    ├── config.ts               # env loading + validation (one place)
    ├── normalize.ts            # normalizeElasticAutoOps + helpers (severity, status, idempotencyKey, alertSignature)
    ├── kafka/
    │   ├── producer.ts         # connect + publishRawEvent + publishNormalizedEvent
    │   └── consumer.ts         # group consumer factory
    ├── couchbase/
    │   ├── client.ts           # cluster connect, bucket/scope/collection handles
    │   └── projection.ts       # historyDocKey, stateDocKey, toHistoryDoc, evolveState
    ├── gateway/
    │   ├── index.ts            # entrypoint — Bun.serve, wires producer + routes
    │   ├── routes.ts           # POST /webhooks/elastic/autoops, GET /healthz
    │   └── schema.ts           # zod payload schema (lenient: handles open/close + High/Medium/Low casing)
    ├── writer/
    │   └── index.ts            # entrypoint — subscribes to events.v1, runs projection, upserts to Couchbase, DLQs on failure
    └── __tests__/
        ├── normalize.test.ts   # severity/status normalization, idempotency key stability, alertSignature, array coercion
        └── projection.test.ts  # evolveState (first event, second open, close-after-open, count increments)
```

## Dependencies

`package.json` (Bun-native scripts):

```json
{
  "name": "eventgate",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev:gateway": "bun run --watch src/gateway/index.ts",
    "dev:writer": "bun run --watch src/writer/index.ts",
    "start:gateway": "bun run src/gateway/index.ts",
    "start:writer": "bun run src/writer/index.ts",
    "test": "bun test",
    "typecheck": "bunx tsc --noEmit"
  },
  "dependencies": {
    "couchbase": "^4.4.0",
    "kafkajs": "^2.2.4",
    "zod": "^3.23.0"
  },
  "devDependencies": {
    "@types/bun": "latest",
    "typescript": "^5.5.0"
  }
}
```

Notes:
- `kafkajs` is the most mature TS Kafka client and works in Bun; using it for both producer and consumer.
- `couchbase` Node SDK works in Bun.
- `zod` for the lenient inbound schema (accepts both `open`/`opened`, `close`/`closed`, both array and CSV for affectedNodes/Indices).
- No Express/Fastify — `Bun.serve` with the routes object handles routing natively.

## Key implementation details

### `src/types.ts`
Adds `ENDPOINT_TYPE` to `ElasticAutoOpsWebhook` (missed in conversation draft). Status normalized to `"opened" | "closed" | "unknown"` internally even though wire format may be `open|close|opened|closed`.

### `src/normalize.ts`
- `normalizeSeverity`: lowercases, maps `high|medium|low` (matches docs' `High|Medium|Low`) → typed union.
- `normalizeStatus`: lowercases, maps both `open`/`opened` → `"opened"` and `close`/`closed` → `"closed"`. This is the one place we accept the docs' actual `open|close` spelling.
- `idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)` — endTime included so a separate close event doesn't collide with the original open.
- `alertSignature = slugify(resourceId :: title)` — stable across open/close pair, used for state rollup.
- Coerces `affectedNodes`/`affectedIndices` from CSV string OR array.

### `src/gateway/routes.ts`
- `POST /webhooks/elastic/autoops`:
  1. Parse JSON.
  2. Validate with zod (`schema.ts`). On failure: 400 with error message, do NOT publish.
  3. Publish raw event to `ops.elastic.autoops.raw.v1` keyed by `resourceId`.
  4. Normalize, publish to `ops.elastic.autoops.events.v1` keyed by `resourceId`.
  5. Return `202` with `{ accepted: true, resourceId, idempotencyKey }`.
  6. On Kafka failure: 503 (operator should retry; AutoOps marks "Notification failed to send" — we want to surface that clearly).
- `GET /healthz`: returns 200 once producer is connected, 503 otherwise.

### `src/writer/index.ts`
- Subscribe to `ops.elastic.autoops.events.v1` with `groupId=autoops-couchbase-writer-v1`.
- For each message:
  1. Parse normalized event.
  2. Upsert `autoops::event::<resourceId>::<occurredAt>::<idempotencyKey>` into `autoops_events` collection (idempotent — same key → same doc).
  3. Get current `autoops::state::<resourceId>::<alertSignature>`, run `evolveState`, upsert.
  4. On unrecoverable error (parse, validation): produce to `ops.elastic.autoops.dlq.v1` and commit offset. On transient Couchbase error: throw, let kafkajs retry.
- `evolveState` already drafted in conversation — re-use as-is.

### `src/config.ts`
Single env reader with sensible defaults so `bun test` doesn't need real services. Variables:
- `PORT` (3000)
- `KAFKA_BROKERS` (required for gateway/writer; tests stub)
- `KAFKA_TOPIC_RAW` (`ops.elastic.autoops.raw.v1`)
- `KAFKA_TOPIC_EVENTS` (`ops.elastic.autoops.events.v1`)
- `KAFKA_TOPIC_DLQ` (`ops.elastic.autoops.dlq.v1`)
- `KAFKA_CLIENT_ID_GATEWAY`, `KAFKA_CLIENT_ID_WRITER`
- `KAFKA_GROUP_ID` (`autoops-couchbase-writer-v1`)
- `COUCHBASE_CONNSTR`, `COUCHBASE_USERNAME`, `COUCHBASE_PASSWORD`
- `COUCHBASE_BUCKET` (`ops`), `COUCHBASE_SCOPE` (`_default`)
- `COUCHBASE_HISTORY_COLLECTION` (`autoops_events`), `COUCHBASE_STATE_COLLECTION` (`autoops_state`)
- `TENANT` (`elastic-cloud`), `ENVIRONMENT` (`prod`)

### `docker-compose.yml`
- **Redpanda** single-node (Kafka-compatible, lighter than Confluent images, no ZK): advertised on `localhost:9092`.
- **Couchbase Server** 7.6 community, with an init script (or README instructions) to create bucket `ops`, scope `_default`, collections `autoops_events` + `autoops_state`.
- Gateway and writer NOT in compose for v1 — operator runs them via `bun run dev:gateway` / `bun run dev:writer` against the compose services. Keeps the inner dev loop fast.

## Tests (`bun test`)

Only the pure functions, no integration tests against real Kafka/Couchbase:

**`normalize.test.ts`**
- `normalizeSeverity` handles `High`/`high`/`HIGH`/missing/garbage.
- `normalizeStatus` handles `open`/`opened`/`Close`/`closed`/missing.
- `severityRank` mapping.
- `idempotencyKey` stable for same inputs, different for any field change including endTime.
- `alertSignature` stable across open + close of same alert (status excluded from signature).
- `affectedNodes`/`affectedIndices` coerce both array and CSV.
- Full normalization snapshot for a representative AutoOps payload.

**`projection.test.ts`**
- `evolveState(null, openedEvent)` → fresh state, openCount=1, isOpen=true.
- `evolveState(prevOpen, secondOpen)` → openCount increments, lastSeenAt updates.
- `evolveState(prevOpen, closedEvent)` → currentStatus="closed", closeCount=1, isOpen=false.
- `historyDocKey`/`stateDocKey` formats.

## Verification

End-to-end smoke test once implemented:

1. `docker compose up -d` — start Redpanda + Couchbase locally.
2. Create the bucket/collections per README instructions (Couchbase UI at `http://localhost:8091`, default creds in `.env.example`).
3. Terminal A: `bun run dev:gateway` — gateway listens on :3000.
4. Terminal B: `bun run dev:writer` — consumer connects to Kafka and Couchbase.
5. Terminal C: send a sample AutoOps payload:
   ```
   curl -X POST http://localhost:3000/webhooks/elastic/autoops \
     -H 'Content-Type: application/json' \
     -d '{"resourceId":"r-123","resourceName":"search-prod-eu","title":"JVM memory pressure high","severity":"High","status":"open","startTime":"2026-05-18T19:27:40Z"}'
   ```
   Expect `202` with the idempotency key.
6. Send the matching `close` event (same `resourceId`+`title`, `status:"close"`, with `endTime`).
7. In Couchbase UI, query:
   - `autoops_events` should contain 2 history docs (open + close).
   - `autoops_state` should contain 1 doc with `currentStatus:"closed"`, `openCount:1`, `closeCount:1`, `isOpen:false`.
8. Send the open event again — confirm history doc is upserted (same key, no duplicate) and state `openCount` increments.
9. Send a malformed payload (missing `resourceId`) — expect `400`, nothing in Kafka.
10. `bun test` passes.

Optional: point an actual AutoOps connector at `https://<tunnel>/webhooks/elastic/autoops` (via cloudflared/ngrok) and click "Validate" in the AutoOps UI.

## Out of scope for v1 (call out in README)

- Webhook authentication — operator can put service behind ALB/Cloudflare Access until v2 adds shared-token header validation. (AutoOps docs confirm no native HMAC/bearer support, only custom headers, so this is an operator-driven config.)
- Flink rolling aggregates.
- Couchbase Kafka Sink Connector (custom consumer is sufficient and gives us projection control).
- Slack/PagerDuty fan-out consumers — separate topic consumers in v3.
- Dockerfile for the apps — added once deployment target is chosen (ECS, k8s, Fly, etc.).
