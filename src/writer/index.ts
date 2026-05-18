import couchbase from "couchbase";
import { config } from "../config.ts";
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

const cb = await connectCouchbase();
const dlqProducer = await createProducer(`${config.kafka.clientIdWriter}-dlq`);
const consumer = await createConsumer(config.kafka.clientIdWriter, config.kafka.groupId);
await consumer.subscribe({ topic: config.kafka.topics.events, fromBeginning: false });

console.log(
  `[writer] consuming ${config.kafka.topics.events} as group=${config.kafka.groupId}`,
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
    } catch (error) {
      const reason = error instanceof Error ? error.message : "parse error";
      console.error("[writer] invalid message, sending to DLQ:", reason);
      await dlqProducer.publishDlq(
        reason,
        raw,
        message.key?.toString() ?? undefined,
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
    } catch (error) {
      if (error instanceof couchbase.DocumentNotFoundError) {
        previous = null;
      } else {
        throw error;
      }
    }
    await cb.state.upsert(stateKey, evolveState(previous, event));
  },
});

async function shutdown(signal: string) {
  console.log(`[writer] received ${signal}, shutting down`);
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
