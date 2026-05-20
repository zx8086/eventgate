// src/health/admin.ts
import { Admin } from "@platformatic/kafka";
import type { KafkaProvider } from "../kafka/providers/index.ts";
import type { AdminLike } from "./probes.ts";

export type HealthAdmin = AdminLike & {
  close(): Promise<void>;
};

export async function createHealthAdmin(provider: KafkaProvider): Promise<HealthAdmin> {
  const conn = await provider.getConnectionConfig();
  const admin = new Admin({
    clientId: `${conn.clientId}-health`,
    bootstrapBrokers: conn.bootstrapBrokers,
    ...(conn.sasl ? { sasl: conn.sasl } : {}),
    ...(conn.tls ? { tls: conn.tls } : {}),
    ...(conn.connectTimeout !== undefined ? { connectTimeout: conn.connectTimeout } : {}),
    ...(conn.timeout !== undefined ? { timeout: conn.timeout } : {}),
  });
  return {
    listTopics: async () => admin.listTopics(),
    close: async () => admin.close(),
  };
}
