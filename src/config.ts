function envOptional(name: string, fallback: string): string {
  const value = process.env[name];
  return value !== undefined && value !== "" ? value : fallback;
}

export const config = {
  port: Number(envOptional("PORT", "3000")),
  tenant: envOptional("TENANT", "elastic-cloud"),
  environment: envOptional("ENVIRONMENT", "prod"),
  kafka: {
    brokers: envOptional("KAFKA_BROKERS", "localhost:9092")
      .split(",")
      .map((b) => b.trim())
      .filter(Boolean),
    clientIdGateway: envOptional("KAFKA_CLIENT_ID_GATEWAY", "eventgate-gateway"),
    clientIdWriter: envOptional("KAFKA_CLIENT_ID_WRITER", "eventgate-writer"),
    groupId: envOptional("KAFKA_GROUP_ID", "autoops-couchbase-writer-v1"),
    topics: {
      raw: envOptional("KAFKA_TOPIC_RAW", "ops.elastic.autoops.raw.v1"),
      events: envOptional("KAFKA_TOPIC_EVENTS", "ops.elastic.autoops.events.v1"),
      dlq: envOptional("KAFKA_TOPIC_DLQ", "ops.elastic.autoops.dlq.v1"),
    },
  },
  couchbase: {
    connStr: envOptional("COUCHBASE_CONNSTR", "couchbase://localhost"),
    username: envOptional("COUCHBASE_USERNAME", "Administrator"),
    password: envOptional("COUCHBASE_PASSWORD", "password"),
    bucket: envOptional("COUCHBASE_BUCKET", "ops"),
    scope: envOptional("COUCHBASE_SCOPE", "_default"),
    historyCollection: envOptional("COUCHBASE_HISTORY_COLLECTION", "autoops_events"),
    stateCollection: envOptional("COUCHBASE_STATE_COLLECTION", "autoops_state"),
  },
} as const;
