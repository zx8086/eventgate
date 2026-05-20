// src/config/loader.ts
import { readFileSync } from "node:fs";
import { defaults } from "./defaults.ts";
import { mapEnv, type EnvOverrides } from "./envMapping.ts";
import { configSchema, type AppConfig } from "./schemas.ts";

// Merge rules:
//   - Plain objects recurse (deep merge per key).
//   - Arrays replace (override array wins wholesale; no concat, no per-index
//     merge). This is the right semantic for every array field we have:
//     `routes` (operator override replaces seed) and
//     `kafka.brokers` (operator CSV replaces default
//     "localhost:9092"). If a future array field needs append semantics, it
//     belongs in its own resolution step, not in this generic merger.
//   - `undefined` overrides are ignored so defaults survive.
function mergeDeep<T extends object>(base: T, overrides: unknown): T {
  if (!overrides || typeof overrides !== "object") return base;
  const out: Record<string, unknown> = { ...(base as Record<string, unknown>) };
  for (const [k, v] of Object.entries(overrides as Record<string, unknown>)) {
    const baseValue = (base as Record<string, unknown>)[k];
    if (
      v !== null &&
      typeof v === "object" &&
      !Array.isArray(v) &&
      baseValue !== null &&
      typeof baseValue === "object" &&
      !Array.isArray(baseValue)
    ) {
      out[k] = mergeDeep(baseValue as object, v);
    } else if (v !== undefined) {
      out[k] = v;
    }
  }
  return out as T;
}

export function buildConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const overrides: EnvOverrides = mapEnv(env);

  // ROUTES_FILE is the only field whose source is the filesystem rather than
  // an env var or default. Resolve it into `overrides.routes` before merge so
  // routes then flow through `mergeDeep` like every other field. Precedence
  // remains ROUTES_FILE > ROUTES_JSON > defaults because the file read
  // overwrites whatever `mapEnv` placed in `overrides.routes`.
  if (overrides.routesFile !== undefined) {
    const raw = readFileSync(overrides.routesFile, "utf8");
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error(`ROUTES_FILE ${overrides.routesFile} must contain a JSON array`);
    }
    overrides.routes = parsed;
  }

  const merged = mergeDeep(defaults as unknown as AppConfig, overrides);

  const result = configSchema.safeParse(merged);
  if (!result.success) {
    const summary = result.error.issues
      .map((i) => `  - ${i.path.join(".") || "<root>"}: ${i.message}`)
      .join("\n");
    throw new Error(`Invalid configuration:\n${summary}`);
  }
  return result.data;
}

let cached: AppConfig | undefined;

function loadOnce(): AppConfig {
  if (!cached) cached = buildConfig();
  return cached;
}

// Lazy Proxy singleton. Importers can `import { config }` at module load
// without triggering validation until a field is actually read.
export const config: AppConfig = new Proxy({} as AppConfig, {
  get(_target, prop) {
    return loadOnce()[prop as keyof AppConfig];
  },
  ownKeys() {
    return Reflect.ownKeys(loadOnce());
  },
  getOwnPropertyDescriptor(_target, prop) {
    return Object.getOwnPropertyDescriptor(loadOnce(), prop);
  },
  has(_target, prop) {
    return prop in loadOnce();
  },
});

export function resetConfigCache(): void {
  cached = undefined;
}
