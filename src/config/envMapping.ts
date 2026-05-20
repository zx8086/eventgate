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
  outbox?: {
    enabled?: boolean;
    dbPath?: string;
    batchSize?: number;
    backoffMaxMs?: number;
    maxAgeHours?: number;
    idlePollMs?: number;
    busyPollMs?: number;
    backlogWarnThreshold?: number;
  };
  admin?: { token?: string };
  routesFile?: string;
  routes?: unknown[];
};

function str(v: string | undefined): string | undefined {
  if (v === undefined) return undefined;
  const trimmed = v.trim();
  return trimmed === "" ? undefined : trimmed;
}

function bool(v: string | undefined): boolean | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  const lower = s.toLowerCase();
  if (lower === "true" || lower === "1" || lower === "yes") return true;
  if (lower === "false" || lower === "0" || lower === "no") return false;
  return undefined;
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

function jsonArray(v: string | undefined): unknown[] | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  try {
    const parsed = JSON.parse(s);
    // Non-array values fall back to defaults rather than crash the process.
    // Operator typos surface via Zod when a valid array is provided instead.
    if (!Array.isArray(parsed)) return undefined;
    return parsed;
  } catch {
    // Malformed JSON falls back to defaults; same reasoning as above.
    return undefined;
  }
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
    outbox: filterUndefined({
      enabled: bool(env.OUTBOX_ENABLED),
      dbPath: str(env.OUTBOX_DB_PATH),
      batchSize: num(env.OUTBOX_BATCH_SIZE),
      backoffMaxMs: num(env.OUTBOX_BACKOFF_MAX_MS),
      maxAgeHours: num(env.OUTBOX_MAX_AGE_HOURS),
      idlePollMs: num(env.OUTBOX_IDLE_POLL_MS),
      busyPollMs: num(env.OUTBOX_BUSY_POLL_MS),
      backlogWarnThreshold: num(env.OUTBOX_BACKLOG_WARN),
    }),
  };

  const routes = jsonArray(env.ROUTES_JSON);
  if (routes !== undefined) {
    overrides.routes = routes;
  }

  const adminToken = str(env.ADMIN_TOKEN);
  if (adminToken !== undefined) {
    overrides.admin = { token: adminToken };
  }

  const routesFileEnv = str(env.ROUTES_FILE);
  if (routesFileEnv !== undefined) {
    overrides.routesFile = routesFileEnv;
  }

  for (const k of Object.keys(overrides) as (keyof EnvOverrides)[]) {
    const section = overrides[k];
    if (
      section &&
      typeof section === "object" &&
      !Array.isArray(section) &&
      Object.keys(section).length === 0
    ) {
      delete overrides[k];
    }
  }
  return overrides;
}
