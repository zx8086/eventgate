// src/config/defaults.ts
import pkg from "../../package.json" with { type: "json" };

export const defaults = {
  app: {
    name: pkg.name,
    version: pkg.version,
    environment: "dev",
  },
  server: {
    port: 3000,
  },
  kafka: {
    provider: "local" as const,
    clientId: "eventgate-gateway",
    brokers: ["localhost:9092"],
    msk: {
      region: "",
      clusterArn: "",
      authMode: "iam" as const,
    },
    confluent: {
      apiKey: "",
      apiSecret: "",
    },
  },
  observability: {
    logLevel: "info" as const,
  },
  outbox: {
    enabled: true,
    dbPath: "./data/outbox.db",
    batchSize: 100,
    backoffMaxMs: 600_000,
    maxAgeHours: 24,
    idlePollMs: 5_000,
    busyPollMs: 250,
    backlogWarnThreshold: 50_000,
  },
  routes: [
    {
      name: "elastic-autoops",
      path: "/webhooks/elastic/autoops",
      topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      keyFields: ["resourceId", "deployment-id"],
      idempotency: "elastic-autoops",
    },
  ],
} as const;

export type Defaults = typeof defaults;
