// src/config/loader.ts
import { readFileSync } from "node:fs";
import { defaults } from "./defaults.ts";
import { mapEnv, type EnvOverrides } from "./envMapping.ts";
import { configSchema, type AppConfig } from "./schemas.ts";

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

  // `routes` resolution: file > env > defaults. Pull `routes` and `routesFile`
  // out of overrides; merge the rest; then attach the resolved routes.
  const { routes: routesOverride, routesFile, ...rest } = overrides;
  const merged = mergeDeep(defaults as unknown as AppConfig, rest);

  let resolvedRoutes: unknown = (defaults as unknown as AppConfig).routes;
  if (routesFile !== undefined) {
    // File source — fail fast on read / parse / shape errors.
    const raw = readFileSync(routesFile, "utf8");
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error(`ROUTES_FILE ${routesFile} must contain a JSON array`);
    }
    resolvedRoutes = parsed;
  } else if (routesOverride !== undefined) {
    resolvedRoutes = routesOverride;
  }

  const withRoutes: AppConfig = {
    ...merged,
    routesFile,
    routes: resolvedRoutes as AppConfig["routes"],
  };

  const result = configSchema.safeParse(withRoutes);
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
