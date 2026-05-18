# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

eventgate is a Bun ingestion service for Elastic AutoOps webhooks. Flow: AutoOps → HTTP gateway → Kafka → Couchbase writer.

## Commands

```bash
bun install                      # install deps
docker compose up -d             # start Redpanda (Kafka) + Couchbase + create topics
bun run dev:gateway              # HTTP receiver, watch mode (port 3000)
bun run dev:writer               # Kafka → Couchbase projector, watch mode
bun run start:gateway            # gateway, no watch
bun run start:writer             # writer, no watch
bun test                         # run all tests
bun test src/__tests__/normalize.test.ts   # run a single test file
bun test -t "buildIdempotencyKey"          # run tests matching a name
bun run typecheck                # tsc --noEmit
```

Before first run after `docker compose up -d`: create bucket `ops` and collections `autoops_events` and `autoops_state` under `_default` scope via Couchbase UI at http://localhost:8091 (Administrator / password). Redpanda creates topics automatically via `redpanda-init`.

Kill background services with `docker compose down` (volumes persist; add `-v` to drop Couchbase data).

## Architecture

Two independent processes share `src/config.ts`, `src/types.ts`, and the Kafka/Couchbase clients. They never call each other in-process — Kafka is the only coupling.

**`src/gateway/index.ts`** — `Bun.serve` HTTP receiver. Routes are built in `gateway/routes.ts` with a Zod schema in `gateway/schema.ts`. On `POST /webhooks/elastic/autoops`: validates body, calls `normalizeElasticAutoOps`, publishes the raw payload to `ops.elastic.autoops.raw.v1` AND the normalized event to `ops.elastic.autoops.events.v1`, returns 202. Returns 503 if Kafka publish fails (gateway must not lose the request silently). `/healthz` reports producer connection state.

**`src/writer/index.ts`** — KafkaJS consumer on `ops.elastic.autoops.events.v1` with group `autoops-couchbase-writer-v1`. For each message: upserts an append-only history doc and upserts a rolling state doc (after a get to read previous state). Parse failures and missing required fields go to `ops.elastic.autoops.dlq.v1` via a separate producer — the consumer never throws on bad messages.

**`src/normalize.ts`** — pure functions, no I/O. The contract the rest of the system depends on:
- `alertSignature = slugify(resourceId :: title)` — groups opened+closed of the same alert into one state doc.
- `idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)` — retries of the same delivery upsert the same history doc; an opened/closed pair produces two distinct history docs because `status` differs.
- AutoOps spells status `open`/`close` in the docs but operators often template `opened`/`closed`. `normalizeStatus` accepts both and emits `opened` | `closed` | `unknown`.

**`src/couchbase/projection.ts`** — pure functions for doc keys and state evolution. `evolveState` is the only place that builds an `AutoOpsStateDoc`; the writer just calls it with the previous doc (or `null`) and the new event. Tests live in `src/__tests__/projection.test.ts`.

**`src/kafka/producer.ts`** — single `createProducer` factory used by both gateway and writer (writer uses it only for DLQ). The returned `EventProducer` exposes `publishRaw` / `publishNormalized` / `publishDlq` so callers never construct Kafka messages directly. Headers on normalized events (`source`, `eventType`, `severity`, `schemaVersion`, `idempotencyKey`) are how downstream fan-out consumers will filter without parsing the body.

**Config** is read once at module load in `src/config.ts` via `envOptional` (no Zod on env — defaults match `.env.example`). Both processes import the same `config` object.

## Conventions specific to this repo

- TS module specifiers include the `.ts` extension (`import { config } from "../config.ts"`) — required by the current `tsconfig` / Bun setup. Keep this when adding imports.
- Pure logic (normalize, projection) is the unit-tested layer. Kafka and Couchbase are exercised manually via the README smoke test, not in `bun test`.
- The writer must stay idempotent — never add side effects that aren't safe to replay, since Kafka retries and consumer rebalances will re-deliver messages.
- New downstream behavior (Slack, PagerDuty, aggregates) should be added as a **separate consumer group** on `ops.elastic.autoops.events.v1`, not bolted into the writer process.

## Out of scope (do not add without discussion)

Webhook auth, Flink aggregates, the Couchbase Kafka Sink Connector, and a Dockerfile are explicitly deferred (see README "Out of scope for v1").
