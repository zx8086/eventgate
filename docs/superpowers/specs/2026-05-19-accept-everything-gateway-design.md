# Design: gateway becomes accept-everything ingestion

**Date:** 2026-05-19
**Status:** Approved by user, ready for implementation plan
**Related:** Supersedes the validation/normalization behaviour added across SIO-789, SIO-790, SIO-795, SIO-799

## Goal

Make the gateway a dumb HTTP-to-Kafka bridge. Any valid JSON POST to `/webhooks/elastic/autoops` is durably persisted to `raw.v1` and acknowledged with 202. Validation, normalization, and downstream decisions become future-consumer concerns.

## Why

Today's gateway returns 400 on Zod failure. AutoOps webhook connectors do not retry; a 400 is permanent data loss. The whole point of the SIO-799 outbox was "don't lose events" — yet schema validation at the HTTP boundary actively throws events away. The current `dlq.v1` topic has been provisioned for >28 hours with zero messages because the gateway treats malformed bodies as "tell the producer," not "preserve and triage downstream."

Webhook ingestion services should accept first, decide later. AutoOps cannot retry; the gateway is the only durable boundary.

## Non-goals

- Building a normalizer service. No downstream consumer of `raw.v1` exists today; specifying one is over-engineering until a real use case appears.
- Schema registry (Confluent Schema Registry, Apicurio, etc.). Overkill for one webhook source.
- Multi-source ingestion. AutoOps is the only producer for now.
- Backfilling `events.v1` from `raw.v1`. Out of scope; if needed, a future consumer can replay.

## Final gateway contract

| Inbound | Outbound |
|---|---|
| POST `/webhooks/elastic/autoops` with valid JSON body | 202 `{ accepted: true }`, one row enqueued to outbox → `raw.v1` |
| POST with non-JSON body | 400 `{ accepted: false, error: "invalid JSON body" }` — protocol failure, not schema |
| Anything else | 404 |
| GET `/healthz` | 200 with `{ producer, outbox }` shape (unchanged from SIO-799) |

The gateway runs:
1. `await req.json()`
2. Compute `idempotencyKey` opportunistically (see below) — best-effort, never throws
3. Outbox enqueue (single row to `raw.v1`)
4. Return 202

No Zod. No `normalize.ts`. No synthetic-Validate detector. No `events.v1` publish. No `dlq.v1` publish.

## Idempotency key (Kafka header on `raw.v1`)

Operational debugging value: lets operators grep / count / correlate identical deliveries by content hash. Stays best-effort so the gateway never depends on AutoOps shape.

New helper `src/gateway/idempotencyKey.ts`:

```ts
// src/gateway/idempotencyKey.ts
import { createHash } from "node:crypto";

function pickString(b: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const k of keys) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return undefined;
}

export function autoOpsIdempotencyKey(body: unknown): string | undefined {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return undefined;
  const b = body as Record<string, unknown>;
  const resourceId = pickString(b, "resourceId", "deployment-id");
  const title = pickString(b, "title");
  const status = pickString(b, "status");
  const startTime = pickString(b, "startTime", "start-time");
  const endTime = pickString(b, "endTime", "end-time");
  if (!resourceId || !title || !status) return undefined;
  return createHash("sha256")
    .update([resourceId, title, status, startTime ?? "", endTime ?? ""].join("::"))
    .digest("hex");
}
```

- Tolerates AutoOps' hyphenated default keys (`deployment-id`, `start-time`, `end-time`) without needing the full mapping table.
- Returns `undefined` when the body doesn't look like an AutoOps event (no `resourceId`, `title`, or `status`).
- Header is attached only when defined.

## Kafka key + headers on `raw.v1`

| Field | Value |
|---|---|
| **key** | `resourceId` (or `"deployment-id"` value), else `"unkeyed"`. Preserves per-resource partition affinity for ordering. |
| **value** | `{ receivedAt: <ISO8601>, raw: <body> }` (unchanged from current envelope) |
| **header** `source` | `"elastic-autoops"` (always) |
| **header** `idempotencyKey` | sha256 hash, present only when `autoOpsIdempotencyKey()` returns a value |

The `key` extraction uses the same `pickString` helper, tolerating hyphenated keys, with `"unkeyed"` fallback so the producer never crashes on weird payloads.

## Code changes

### Delete

- `src/normalize.ts`
- `src/gateway/schema.ts`
- The `NormalizedEvent`, `NormalizedAlert`, `Severity`, `SeverityRank`, `EventType` types from `src/types.ts` (anything only the writer/normalizer used)
- `test/unit/normalize.test.ts` (or whatever the current path is)
- Any other tests that exercised the deleted modules

If you ever need this logic back, `git log -p src/normalize.ts` is the reference. The most recent version is correct (SIO-790 fixed the `RESOLVED` mapping).

### Modify

**`src/gateway/routes.ts`** — strip validation, normalization, synthetic detector, pair-enqueue. Down to a 30-line handler:

```ts
// src/gateway/routes.ts
import { config } from "../config/index.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { autoOpsIdempotencyKey } from "./idempotencyKey.ts";

const log = getLogger("gateway.routes");

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
};

function pickKey(body: unknown): string {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "unkeyed";
  const b = body as Record<string, unknown>;
  for (const k of ["resourceId", "deployment-id"]) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return "unkeyed";
}

export function buildRoutes(deps: RouteDeps) {
  const { producer, outbox } = deps;

  return {
    "/healthz": () => {
      const stats = outbox?.backlogStats();
      const producerOk = producer.isConnected();
      return Response.json(
        {
          ok: producerOk,
          producer: { connected: producerOk },
          outbox: stats
            ? {
                enabled: true,
                pending: stats.pending,
                failed: stats.failed,
                oldestPendingAgeMs: stats.oldestPendingAgeMs,
              }
            : { enabled: false },
        },
        { status: producerOk ? 200 : 503 },
      );
    },

    "/webhooks/elastic/autoops": {
      POST: async (req: Request) => {
        let body: unknown;
        try {
          body = await req.json();
        } catch {
          return Response.json(
            { accepted: false, error: "invalid JSON body" },
            { status: 400 },
          );
        }

        const messageKey = pickKey(body);
        const idempotencyKey = autoOpsIdempotencyKey(body);
        const headers: Record<string, string> = { source: "elastic-autoops" };
        if (idempotencyKey) headers.idempotencyKey = idempotencyKey;

        const enqueueInput = {
          topic: "raw" as const,
          messageKey,
          payload: JSON.stringify({
            receivedAt: new Date().toISOString(),
            raw: body,
          }),
          headers,
        };

        if (outbox) {
          try {
            outbox.enqueue(enqueueInput);
          } catch (err) {
            log.error({ err, messageKey }, "outbox enqueue failed");
            return Response.json(
              { accepted: false, error: "outbox enqueue failed" },
              { status: 500 },
            );
          }
        } else {
          try {
            await producer.publishRaw(messageKey, body);
          } catch (err) {
            log.warn({ err, messageKey }, "kafka publish failed; will not retry without outbox");
          }
        }

        return Response.json({ accepted: true }, { status: 202 });
      },
    },
  };
}
```

**`src/outbox/writer.ts`** — split the existing `enqueuePair(raw, normalized)` into `enqueue(row)` for the single-row case. Keep `enqueuePair` only if backwards-compat matters; otherwise delete it. (We are the only caller — delete.)

```ts
// src/outbox/writer.ts (relevant change)
export type OutboxWriter = {
  enqueue(row: EnqueueInput): void;
  backlogStats(): BacklogStats;
};

export function createWriter(db: OutboxDatabase): OutboxWriter {
  // existing insertRow query
  const enqueueTx = db.transaction((row: EnqueueInput) => {
    const now = Date.now();
    const topic = outboxTopicSchema.parse(row.topic);
    insertRow.run({ /* same fields, single row */ });
  });
  return {
    enqueue(row) { enqueueTx(row); },
    backlogStats() { /* unchanged */ },
  };
}
```

**`src/kafka/producer.ts`** — `publishNormalized` and `publishDlq` lose their only caller. Either delete them (simplest, matches "gateway never writes events.v1 or dlq.v1") or leave them for the drainer's `sendByTopic` indirection. The drainer already routes via `topicToKafka(topic, topics)`, so the producer can keep the topic-agnostic `sendByTopic` and drop the typed `publishNormalized`/`publishDlq`. **Delete `publishNormalized` and `publishDlq`.**

**`src/outbox/schemas.ts`** — `OutboxTopic` can stay as `"raw" | "events" | "dlq"` even though only `"raw"` is used today, so future code that wants to enqueue to the other topics doesn't have to widen the type. Or narrow to `"raw"` only for honesty. **Narrow to `"raw"`** — less misleading. If a future consumer wants to enqueue, it adds the variant back deliberately.

**`src/outbox/drainer.ts`** — `topicToKafka()` switch shrinks to one case. Trivial.

**`src/config/schemas.ts`** — `kafka.topics` keeps all three topic names (operators may still want to inspect `events.v1`/`dlq.v1` for historical messages from before this change). No env-var changes.

**`src/types.ts`** — delete the orphan types. Keep `ElasticAutoOpsWebhook` if it's exported anywhere; otherwise delete too.

### Add

- `src/gateway/idempotencyKey.ts` (above)
- `test/unit/idempotencyKey.test.ts` — 4 tests:
  1. Returns stable hash for a complete AutoOps body (camelCase keys)
  2. Returns the same hash for hyphenated equivalents (`deployment-id` → same hash as `resourceId`)
  3. Returns `undefined` when `resourceId`, `title`, or `status` is missing
  4. Different bodies → different hashes
- `test/unit/gateway.routes.test.ts` (new or simplified) — three tests:
  1. Non-JSON body → 400
  2. Valid JSON without AutoOps shape → 202, enqueue called with no `idempotencyKey` header
  3. Valid AutoOps body → 202, enqueue called with `idempotencyKey` header present, key derived from `resourceId`

## Topics

| Topic | After this change |
|---|---|
| `ops.elastic.autoops.raw.v1` | Sole gateway output. All accepted traffic lands here. |
| `ops.elastic.autoops.events.v1` | **Unused by the gateway.** Reserved for future consumers that want to publish a normalized stream. README documents this. |
| `ops.elastic.autoops.dlq.v1` | **Unused by the gateway.** Reserved for future consumers that quarantine messages they cannot process. README documents this. |

The two reserved topics stay provisioned. Empty-but-defined topics signal architectural intent at near-zero cost (a few KB of Redpanda metadata).

## Documentation updates

- `README.md` — replace the existing flow diagram and "what eventgate does" section with the accept-everything contract. State explicitly that `events.v1` and `dlq.v1` are reserved for future consumers and have no producer today.
- `docs/architecture/overview.md` — update Data Flow, Failure Handling, and Normalization Contract sections. Normalization Contract goes away entirely. Failure Handling collapses to two rows (non-JSON → 400; everything else → 202).
- `docs/configuration/environment-variables.md` — no changes; the env vars are unchanged.
- `docs/architecture/outbox.md` — single-row enqueue replaces pair-enqueue. Update the example.
- `CLAUDE.md` — strip mentions of `normalize.ts`, `schema.ts`, the Zod schema. Restate the contract.

## Migration / backwards compatibility

- **`raw.v1` envelope is unchanged** — `{ receivedAt, raw }`. Existing messages on the topic remain readable.
- **`events.v1` stops receiving new messages.** Historical messages stay. The writer service is already gone (deleted earlier today), so no consumer breaks.
- **`dlq.v1` was always empty.** No change.
- **HTTP 202 body simplifies** from `{ accepted, resourceId, idempotencyKey }` to `{ accepted: true }`. AutoOps doesn't read the body, so this is safe. Document in the release notes anyway for anyone calling the endpoint manually.

## Verification (post-deploy)

1. `curl http://<alb>/healthz` → 200 with the new outbox shape (unchanged).
2. `curl -X POST <alb>/webhooks/elastic/autoops -d '{"resourceId":"x","title":"t","status":"open"}'` → 202 `{ accepted: true }`. CloudWatch shows an outbox-info line for the enqueue. Redpanda `rpk topic consume ops.elastic.autoops.raw.v1 --offset end-1` shows the message with `headers: { source: "elastic-autoops", idempotencyKey: "<hash>" }`.
3. `curl -X POST <alb>/webhooks/elastic/autoops -d '{"hello":"world"}'` → 202. Message lands on `raw.v1` with **only** `source` header (no idempotencyKey).
4. `curl -X POST <alb>/webhooks/elastic/autoops -d 'not json'` → 400.
5. AutoOps "Validate" body (all `${VAR}` placeholders) → 202. Message lands on `raw.v1` with no `idempotencyKey` header. (This replaces the synthetic-detector fast path with the same observable outcome — Validate succeeds — but the body now persists. Acceptable: it's one message per click and easy to filter downstream.)
6. AutoOps real event → 202. Verify in Redpanda that the `idempotencyKey` header matches what the same body would have produced under the old code (regression check: run the old `normalize.ts`'s hash function locally on a sample body, compare).
7. Send the same body twice. Both messages land on `raw.v1` with the **same** `idempotencyKey` header — that is the property the user cares about (spotting repeats).

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| `raw.v1` retention isn't long enough for future consumers to backfill from | Medium | Set Redpanda topic retention explicitly to a generous value (e.g., 30 days). Current cluster defaults to ~24h. Add an `rpk topic alter-config` call to the deploy script. |
| Operator clicks AutoOps Validate hundreds of times and floods `raw.v1` with junk | Low | Junk bodies are tiny (~500B each). Even 10k clicks = ~5MB. Negligible. Documented as expected behaviour. |
| Future consumer can't tell AutoOps real events from Validate test bodies | Low | The synthetic body has all-placeholder values (`${RESOURCE_ID}` etc.); easy heuristic for the consumer. Also the `idempotencyKey` header is absent on synthetic bodies (all field values fail the non-empty check). |
| Reviewer asks why we deleted ~500 lines of "working" code | Medium | Spec lead: the gateway is not the right place for this logic. Logic preserved in git history; not deleted from history, just from the runtime path. |
| Existing alarms / dashboards that watched `events.v1` topic depth go silent | Low | None known to exist. Audit before merge. |

## Out of scope (do not bundle)

- Schema versioning headers (`schemaVersion`) — let the future consumer that produces `events.v1` set these.
- A second producer to `raw.v1` (e.g., manual replay endpoint) — separate ticket if/when needed.
- Retention policy automation — flagged in Risks; do once, not as part of this change.
- Topic ACLs — not relevant under single-cluster Redpanda; revisit when moving to managed Kafka.

## Related code / history references

- `src/gateway/routes.ts` — primary change site
- `src/gateway/schema.ts` — to delete (last touched: SIO-790, `accept-resolved-status-as-closed`)
- `src/normalize.ts` — to delete (last touched: SIO-790)
- `src/outbox/writer.ts:30` — `enqueuePair` becomes `enqueue`
- `src/outbox/drainer.ts:topicToKafka` — switch shrinks
- Commit `e520820` — original two-process design that introduced the normalization logic
- Commit `15ed243` (SIO-795) — when the writer process was deleted; this spec completes the cleanup the writer's removal implied
- Commit `49992a0` (SIO-799) — outbox; pair-enqueue becomes single-enqueue

## Linear

Create `SIO-XXX: gateway accept-everything (delete validation, single-row outbox)` in the Event Gate project. Single PR. Estimated half-day of work plus review.
