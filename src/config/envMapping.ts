// src/config/envMapping.ts
type RawEnv = NodeJS.ProcessEnv;

export type EnvOverrides = {
  app?: { environment?: string };
  server?: { port?: number };
  kafka?: {
    provider?: string;
    clientId?: string;
    brokers?: string[];
    msk?: { region?: string; clusterArn?: string; authMode?: string };
    confluent?: { apiKey?: string; apiSecret?: string };
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
  health?: {
    probeIntervalMs?: number;
    probeTimeoutMs?: number;
    heartbeatMs?: number;
  };
  breaker?: {
    failureThreshold?: number;
    successThreshold?: number;
    recoveryTimeoutMs?: number;
  };
  admin?: { token?: string };
  routesFile?: string;
  routes?: unknown[];
  replay?: {
    enabled?: boolean;
    maxAttempts?: number;
    transientErrors?: string[];
    poisonErrors?: string[];
    default?: string;
    maxRecordsPerJob?: number;
    rateLimitPerSec?: number;
    parkingTopicSuffix?: string;
    auto?: {
      enabled?: boolean;
      intervalMs?: number;
      dlqDepthThreshold?: number;
      probeWindowRecords?: number;
    };
  };
};

// Defaults applied when the replay block is attached. Lives here so the
// schema (which makes `replay` optional) stays the single source of truth on
// presence/absence; only the values are filled in.
const REPLAY_DEFAULTS = {
  enabled: false,
  maxAttempts: 5,
  transientErrors: [] as string[],
  poisonErrors: [] as string[],
  default: "park" as const,
  maxRecordsPerJob: 10_000,
  rateLimitPerSec: 500,
  parkingTopicSuffix: ".parked",
  auto: {
    enabled: false,
    intervalMs: 300_000,
    dlqDepthThreshold: 100,
    probeWindowRecords: 500,
  },
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
    }),
    server: filterUndefined({
      port: num(env.PORT),
    }),
    kafka: filterUndefined({
      provider: str(env.KAFKA_PROVIDER),
      clientId: str(env.KAFKA_CLIENT_ID),
      brokers: csv(env.KAFKA_BROKERS),
      msk: nestedOrUndefined({
        region: str(env.MSK_REGION),
        clusterArn: str(env.MSK_CLUSTER_ARN),
        authMode: str(env.MSK_AUTH_MODE),
      }),
      confluent: nestedOrUndefined({
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
    health: filterUndefined({
      probeIntervalMs: num(env.HEALTH_PROBE_INTERVAL_MS),
      probeTimeoutMs: num(env.HEALTH_PROBE_TIMEOUT_MS),
      heartbeatMs: num(env.STATS_HEARTBEAT_MS),
    }),
    breaker: filterUndefined({
      failureThreshold: num(env.CIRCUIT_BREAKER_FAILURE_THRESHOLD),
      successThreshold: num(env.CIRCUIT_BREAKER_SUCCESS_THRESHOLD),
      recoveryTimeoutMs: num(env.CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS),
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

  // Replay block: attach iff REPLAY_ENABLED is truthy OR any REPLAY_* sub-key
  // is set. When attached, fill the full schema with defaults and overlay env
  // overrides. When absent, config.replay stays undefined (feature off).
  const replayEnabled = bool(env.REPLAY_ENABLED);
  const replayMaxAttempts = num(env.REPLAY_MAX_ATTEMPTS);
  const replayTransient = csv(env.REPLAY_TRANSIENT_ERRORS);
  const replayPoison = csv(env.REPLAY_POISON_ERRORS);
  const replayDefault = str(env.REPLAY_DEFAULT);
  const replayMaxRecords = num(env.REPLAY_MAX_RECORDS_PER_JOB);
  const replayRateLimit = num(env.REPLAY_RATE_LIMIT_PER_SEC);
  const replayParkingSuffix = str(env.REPLAY_PARKING_TOPIC_SUFFIX);
  const replayAutoEnabled = bool(env.REPLAY_AUTO_ENABLED);
  const replayAutoInterval = num(env.REPLAY_AUTO_INTERVAL_MS);
  const replayAutoThreshold = num(env.REPLAY_AUTO_DLQ_DEPTH_THRESHOLD);
  const replayAutoProbe = num(env.REPLAY_AUTO_PROBE_WINDOW_RECORDS);

  const anyReplaySubkey =
    replayMaxAttempts !== undefined ||
    replayTransient !== undefined ||
    replayPoison !== undefined ||
    replayDefault !== undefined ||
    replayMaxRecords !== undefined ||
    replayRateLimit !== undefined ||
    replayParkingSuffix !== undefined ||
    replayAutoEnabled !== undefined ||
    replayAutoInterval !== undefined ||
    replayAutoThreshold !== undefined ||
    replayAutoProbe !== undefined;

  if (replayEnabled !== undefined || anyReplaySubkey) {
    overrides.replay = {
      ...REPLAY_DEFAULTS,
      ...(replayEnabled !== undefined ? { enabled: replayEnabled } : {}),
      ...(replayMaxAttempts !== undefined ? { maxAttempts: replayMaxAttempts } : {}),
      ...(replayTransient !== undefined ? { transientErrors: replayTransient } : {}),
      ...(replayPoison !== undefined ? { poisonErrors: replayPoison } : {}),
      ...(replayDefault !== undefined ? { default: replayDefault } : {}),
      ...(replayMaxRecords !== undefined ? { maxRecordsPerJob: replayMaxRecords } : {}),
      ...(replayRateLimit !== undefined ? { rateLimitPerSec: replayRateLimit } : {}),
      ...(replayParkingSuffix !== undefined ? { parkingTopicSuffix: replayParkingSuffix } : {}),
      auto: {
        ...REPLAY_DEFAULTS.auto,
        ...(replayAutoEnabled !== undefined ? { enabled: replayAutoEnabled } : {}),
        ...(replayAutoInterval !== undefined ? { intervalMs: replayAutoInterval } : {}),
        ...(replayAutoThreshold !== undefined ? { dlqDepthThreshold: replayAutoThreshold } : {}),
        ...(replayAutoProbe !== undefined ? { probeWindowRecords: replayAutoProbe } : {}),
      },
    };
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
