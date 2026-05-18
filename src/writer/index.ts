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

const cb = await connectCouchbase();
const dlqProducer = await createProducer(`${config.kafka.clientIdWriter}-dlq`);
const consumer = await createConsumer(config.kafka.clientIdWriter, config.kafka.groupId);
await consumer.subscribe({ topic: config.kafka.topics.events, fromBeginning: false });

log.info(
  { topic: config.kafka.topics.events, groupId: config.kafka.groupId },
  "writer consuming",
);

await consumer.run({
  eachMessage: async ({ message }) => {
    if (!message.value) return;
    const raw = message.value.toString();

    let event: NormalizedEvent;
    try {
      event = JSON.parse(raw) as NormalizedEvent;
      if (!event?.resource?.id || !event?.idempotencyKey) {
        throw new Error("missing required normalized fields");
      }
    } catch (err) {
      const reason = err instanceof Error ? err.message : "parse error";
      log.warn({ err, reason }, "invalid message, sending to DLQ");
      await dlqProducer.publishDlq(reason, raw, message.key?.toString() ?? undefined);
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
  },
});

async function shutdown(signal: string) {
  log.info({ signal }, "shutting down writer");
  try {
    await consumer.disconnect();
    await dlqProducer.disconnect();
    await cb.close();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
