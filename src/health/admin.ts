// src/health/admin.ts
import { Admin } from "@platformatic/kafka";
import type { KafkaProvider } from "../kafka/providers/index.ts";
import type { AdminLike } from "./probes.ts";

// HealthAdmin extends the probe surface with the methods replay needs:
// - metadata + listOffsets for per-partition DLQ depth (dlqInspector)
// - deleteGroups for per-job consumer-group cleanup (replay/consumer)
// Reuses the single long-lived Admin client so we don't spin up a new one
// per replay job.
export type HealthAdmin = AdminLike & {
  close(): Promise<void>;
  metadata: Admin["metadata"];
  listOffsets: Admin["listOffsets"];
  deleteGroups: Admin["deleteGroups"];
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
    metadata: admin.metadata.bind(admin),
    listOffsets: admin.listOffsets.bind(admin),
    deleteGroups: admin.deleteGroups.bind(admin),
    close: async () => admin.close(),
  };
}
