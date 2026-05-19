// src/config/envMapping.ts
type RawEnv = NodeJS.ProcessEnv;

export type EnvOverrides = {
  app?: { environment?: string; tenant?: string };
  server?: { port?: number };
  kafka?: {
    provider?: string;
    clientId?: string;
    topics?: { raw?: string; events?: string; dlq?: string };
    local?: { bootstrapServers?: string[] };
    msk?: { region?: string; clusterArn?: string; brokers?: string; authMode?: string };
    confluent?: { bootstrapServers?: string; apiKey?: string; apiSecret?: string };
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

function nestedOrUndefined<T extends object>(obj: T): Partial<T> | undefined {
  const filtered = filterUndefined(obj);
  return Object.keys(filtered).length > 0 ? filtered : undefined;
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
      provider: str(env.KAFKA_PROVIDER),
      clientId: str(env.KAFKA_CLIENT_ID),
      topics: nestedOrUndefined({
        raw: str(env.KAFKA_TOPIC_RAW),
        events: str(env.KAFKA_TOPIC_EVENTS),
        dlq: str(env.KAFKA_TOPIC_DLQ),
      }),
      local: nestedOrUndefined({
        bootstrapServers: csv(env.KAFKA_LOCAL_BOOTSTRAP_SERVERS),
      }),
      msk: nestedOrUndefined({
        region: str(env.MSK_REGION),
        clusterArn: str(env.MSK_CLUSTER_ARN),
        brokers: str(env.MSK_BROKERS),
        authMode: str(env.MSK_AUTH_MODE),
      }),
      confluent: nestedOrUndefined({
        bootstrapServers: str(env.CONFLUENT_BOOTSTRAP_SERVERS),
        apiKey: str(env.CONFLUENT_API_KEY),
        apiSecret: str(env.CONFLUENT_API_SECRET),
      }),
    }),
    observability: filterUndefined({
      logLevel: str(env.LOG_LEVEL),
    }),
  };

  for (const k of Object.keys(overrides) as (keyof EnvOverrides)[]) {
    const section = overrides[k];
    if (section && typeof section === "object" && Object.keys(section).length === 0) {
      delete overrides[k];
    }
  }
  return overrides;
}
