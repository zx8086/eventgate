// src/kafka/producer.ts
import {
  Producer,
  stringSerializer,
  type Serializers,
} from "@platformatic/kafka";
import type { KafkaProvider } from "./providers/index.ts";

// ProducerHeaders supports two shapes:
//   - Record<string,string>   — used by the webhook handler + outbox drainer.
//   - Array<[string, ...]>    — used by the replay subsystem (Phase 1+) so
//                                preserved Buffer values (e.g. binary
//                                idempotencyKey) survive bit-for-bit.
// Tuples with a null value are SKIPPED — the underlying SDK doesn't accept
// null header values. Preserve fidelity at the caller (replay re-produce
// drops null-value tuples explicitly with a debug log) rather than smuggling
// them through here.
export type ProducerHeaders =
  | Record<string, string>
  | Array<[string, string | Buffer | null]>
  | null;

export function normalizeHeaders(
  headers: ProducerHeaders | undefined,
): Map<Buffer, Buffer> | undefined {
  if (headers === undefined || headers === null) return undefined;
  const map = new Map<Buffer, Buffer>();
  if (Array.isArray(headers)) {
    for (const [k, v] of headers) {
      if (v === null) continue;
      const buf = Buffer.isBuffer(v) ? v : Buffer.from(v, "utf-8");
      map.set(Buffer.from(k, "utf-8"), buf);
    }
    return map;
  }
  for (const [k, v] of Object.entries(headers)) {
    map.set(Buffer.from(k, "utf-8"), Buffer.from(v, "utf-8"));
  }
  return map;
}

// Buffer-typed header serializers so the Producer<string, string, Buffer, Buffer>
// accepts the Map<Buffer, Buffer | null> emitted by normalizeHeaders.
const bufferHeaderSerializer = (data?: Buffer): Buffer | undefined => data;

const valueSerializers: Serializers<string, string, Buffer, Buffer> = {
  key: stringSerializer,
  value: stringSerializer,
  headerKey: bufferHeaderSerializer,
  headerValue: bufferHeaderSerializer,
};

export type EventProducer = {
  sendByTopic(
    topic: string,
    key: string,
    value: string,
    headers?: ProducerHeaders,
  ): Promise<void>;
  isConnected(): boolean;
  disconnect(): Promise<void>;
};

export async function createProducer(
  clientId: string,
  provider: KafkaProvider,
): Promise<EventProducer> {
  const conn = await provider.getConnectionConfig();
  const producer = new Producer<string, string, Buffer, Buffer>({
    clientId,
    bootstrapBrokers: conn.bootstrapBrokers,
    ...(conn.sasl ? { sasl: conn.sasl } : {}),
    ...(conn.tls ? { tls: conn.tls } : {}),
    ...(conn.connectTimeout !== undefined ? { connectTimeout: conn.connectTimeout } : {}),
    ...(conn.timeout !== undefined ? { timeout: conn.timeout } : {}),
    ...(conn.retries !== undefined ? { retries: conn.retries } : {}),
    ...(conn.retryDelay !== undefined ? { retryDelay: conn.retryDelay } : {}),
    serializers: valueSerializers,
  });

  let connected = true;

  return {
    isConnected: () => connected,
    async disconnect() {
      connected = false;
      await producer.close();
    },
    async sendByTopic(topic, key, value, headers) {
      const normalized = normalizeHeaders(headers);
      await producer.send({
        messages: [
          {
            topic,
            key,
            value,
            ...(normalized !== undefined ? { headers: normalized } : {}),
          },
        ],
      });
    },
  };
}
