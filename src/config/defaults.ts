// src/config/defaults.ts
import pkg from "../../package.json" with { type: "json" };

export const defaults = {
  app: {
    name: pkg.name,
    version: pkg.version,
    environment: "dev",
    tenant: "elastic-cloud",
  },
  server: {
    port: 3000,
  },
  kafka: {
    provider: "local" as const,
    clientId: "eventgate-gateway",
    topics: {
      raw: "ops.elastic.autoops.raw.v1",
      events: "ops.elastic.autoops.events.v1",
      dlq: "ops.elastic.autoops.dlq.v1",
    },
    local: {
      bootstrapServers: ["localhost:9092"],
    },
    msk: {
      region: "",
      clusterArn: "",
      brokers: "",
      authMode: "iam" as const,
    },
    confluent: {
      bootstrapServers: "",
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
} as const;

export type Defaults = typeof defaults;
