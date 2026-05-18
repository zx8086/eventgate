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
    brokers: ["localhost:9092"],
    clientIdGateway: "eventgate-gateway",
    clientIdWriter: "eventgate-writer",
    groupId: "autoops-couchbase-writer-v1",
    topics: {
      raw: "ops.elastic.autoops.raw.v1",
      events: "ops.elastic.autoops.events.v1",
      dlq: "ops.elastic.autoops.dlq.v1",
    },
  },
  couchbase: {
    connStr: "couchbase://localhost",
    username: "Administrator",
    password: "password",
    bucket: "ops",
    scope: "_default",
    historyCollection: "autoops_events",
    stateCollection: "autoops_state",
  },
  observability: {
    logLevel: "info" as const,
  },
} as const;

export type Defaults = typeof defaults;
