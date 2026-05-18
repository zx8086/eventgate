// src/config/envMapping.ts
// Explicit mapping of env vars to the typed config shape.
// Returns a deep-partial; loader merges this over defaults.

type RawEnv = NodeJS.ProcessEnv;

export type EnvOverrides = {
  app?: { environment?: string; tenant?: string };
  server?: { port?: number };
  kafka?: {
    brokers?: string[];
    clientIdGateway?: string;
    clientIdWriter?: string;
    groupId?: string;
    topics?: { raw?: string; events?: string; dlq?: string };
  };
  couchbase?: {
    connStr?: string;
    username?: string;
    password?: string;
    bucket?: string;
    scope?: string;
    historyCollection?: string;
    stateCollection?: string;
  };
  observability?: { logLevel?: string };
};

function str(v: string | undefined): string | undefined {
  return v !== undefined && v !== "" ? v : undefined;
}

function num(v: string | undefined): number | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  const n = Number(s);
  return Number.isFinite(n) ? n : undefined;
}

function csv(v: string | undefined): string[] | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  const parts = s.split(",").map((p) => p.trim()).filter(Boolean);
  return parts.length > 0 ? parts : undefined;
}

function filterUndefined<T extends object>(obj: T): Partial<T> {
  const out: Partial<T> = {};
  for (const key of Object.keys(obj) as (keyof T)[]) {
    if (obj[key] !== undefined) out[key] = obj[key];
  }
  return out;
}

export function mapEnv(env: RawEnv): EnvOverrides {
  const overrides: EnvOverrides = {
    app: filterUndefined({
      environment: str(env.ENVIRONMENT),
      tenant: str(env.TENANT),
    }),
    server: filterUndefined({
      port: num(env.PORT),
    }),
    kafka: filterUndefined({
      brokers: csv(env.KAFKA_BROKERS),
      clientIdGateway: str(env.KAFKA_CLIENT_ID_GATEWAY),
      clientIdWriter: str(env.KAFKA_CLIENT_ID_WRITER),
      groupId: str(env.KAFKA_GROUP_ID),
      topics: filterUndefined({
        raw: str(env.KAFKA_TOPIC_RAW),
        events: str(env.KAFKA_TOPIC_EVENTS),
        dlq: str(env.KAFKA_TOPIC_DLQ),
      }),
    }),
    couchbase: filterUndefined({
      connStr: str(env.COUCHBASE_CONNSTR),
      username: str(env.COUCHBASE_USERNAME),
      password: str(env.COUCHBASE_PASSWORD),
      bucket: str(env.COUCHBASE_BUCKET),
      scope: str(env.COUCHBASE_SCOPE),
      historyCollection: str(env.COUCHBASE_HISTORY_COLLECTION),
      stateCollection: str(env.COUCHBASE_STATE_COLLECTION),
    }),
    observability: filterUndefined({
      logLevel: str(env.LOG_LEVEL),
    }),
  };

  // Strip empty sections so the merge is clean.
  for (const k of Object.keys(overrides) as (keyof EnvOverrides)[]) {
    const section = overrides[k];
    if (section && typeof section === "object" && Object.keys(section).length === 0) {
      delete overrides[k];
    }
  }
  return overrides;
}
