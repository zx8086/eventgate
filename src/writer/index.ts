// src/writer/index.ts
import couchbase from "couchbase";
import { config } from "../config/index.ts";
import { getLogger } from "../logging/index.ts";
import { connectCouchbase } from "../couchbase/client.ts";
import {
  evolveState,
  historyDocKey,
  stateDocKey,
  toHistoryDoc,
} from "../couchbase/projection.ts";
import { createConsumer } from "../kafka/consumer.ts";
import { createProducer } from "../kafka/producer.ts";
import type { AutoOpsStateDoc, NormalizedEvent } from "../types.ts";

const log = getLogger("writer");

const cb = config.couchbase.enabled ? await connectCouchbase() : null;
if (!cb) {
  log.warn(
    { couchbaseEnabled: false },
    "couchbase disabled; events will be logged but not persisted",
  );
}

const dlqProducer = await createProducer(`${config.kafka.clientIdWriter}-dlq`);
const consumer = createConsumer(config.kafka.clientIdWriter, config.kafka.groupId);

const stream = await consumer.consume({
  topics: [config.kafka.topics.events],
});

log.info(
  { topic: config.kafka.topics.events, groupId: config.kafka.groupId },
  "writer consuming",
);

stream.on("data", (message) => {
  void handleMessage(message);
});
stream.on("error", (err) => {
  log.error({ err }, "consumer stream error");
});

async function handleMessage(message: {
  key: string | null;
  value: string | null;
  headers: Map<string, string>;
}): Promise<void> {
  if (!message.value) return;
  const raw = message.value;

  let event: NormalizedEvent;
  try {
    event = JSON.parse(raw) as NormalizedEvent;
    if (!event?.resource?.id || !event?.idempotencyKey) {
      throw new Error("missing required normalized fields");
    }
  } catch (err) {
    const reason = err instanceof Error ? err.message : "parse error";
    log.warn({ err, reason }, "invalid message, sending to DLQ");
    await dlqProducer.publishDlq(reason, raw, message.key ?? undefined);
    return;
  }

  if (!cb) {
    log.info(
      {
        resourceId: event.resource.id,
        alertSignature: event.alert.alertSignature,
        idempotencyKey: event.idempotencyKey,
        status: event.alert.status,
        severity: event.alert.severity,
      },
      "normalized event received (couchbase disabled)",
    );
    return;
  }

  const historyKey = historyDocKey(event);
  await cb.history.upsert(historyKey, toHistoryDoc(event));

  const stateKey = stateDocKey(event);
  let previous: AutoOpsStateDoc | null = null;
  try {
    const existing = await cb.state.get(stateKey);
    previous = existing.content as AutoOpsStateDoc;
  } catch (err) {
    if (err instanceof couchbase.DocumentNotFoundError) {
      previous = null;
    } else {
      throw err;
    }
  }
  await cb.state.upsert(stateKey, evolveState(previous, event));
}

async function shutdown(signal: string) {
  log.info({ signal }, "shutting down writer");
  try {
    stream.destroy();
    await consumer.close();
    await dlqProducer.disconnect();
    if (cb) await cb.close();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
