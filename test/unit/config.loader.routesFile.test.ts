// test/unit/config.loader.routesFile.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildConfig } from "../../src/config/loader.ts";

let snapshot: NodeJS.ProcessEnv;
let dir: string;
const baseEnv = { ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };

beforeEach(() => {
  snapshot = { ...process.env };
  dir = mkdtempSync(join(tmpdir(), "eventgate-loader-"));
});
afterEach(() => {
  process.env = snapshot;
  if (dir) rmSync(dir, { recursive: true, force: true });
});

const fileRoute = [
  {
    name: "file-route",
    path: "/webhooks/file",
    topic: "T_PRIVATE_SOURCE_FILE_X",
    keyFields: ["id"],
  },
];

const envRoute = [
  {
    name: "env-route",
    path: "/webhooks/env",
    topic: "T_PRIVATE_SOURCE_ENV_X",
    keyFields: ["id"],
  },
];

describe("buildConfig routes source precedence", () => {
  it("uses ROUTES_FILE when set and present, ignoring ROUTES_JSON", () => {
    const path = join(dir, "routes.json");
    writeFileSync(path, JSON.stringify(fileRoute));
    const cfg = buildConfig({
      ...baseEnv,
      ROUTES_FILE: path,
      ROUTES_JSON: JSON.stringify(envRoute),
    });
    expect(cfg.routes[0]?.name).toBe("file-route");
  });

  it("falls through to ROUTES_JSON when ROUTES_FILE is unset", () => {
    const cfg = buildConfig({
      ...baseEnv,
      ROUTES_JSON: JSON.stringify(envRoute),
    });
    expect(cfg.routes[0]?.name).toBe("env-route");
  });

  it("falls through to defaults when neither is set", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.routes[0]?.name).toBe("elastic-autoops");
  });

  it("throws when ROUTES_FILE is set but the file does not exist", () => {
    expect(() => buildConfig({ ...baseEnv, ROUTES_FILE: join(dir, "missing.json") })).toThrow();
  });

  it("throws when ROUTES_FILE points at non-JSON", () => {
    const path = join(dir, "bad.json");
    writeFileSync(path, "not json");
    expect(() => buildConfig({ ...baseEnv, ROUTES_FILE: path })).toThrow();
  });

  it("throws when ROUTES_FILE points at a JSON object instead of array", () => {
    const path = join(dir, "obj.json");
    writeFileSync(path, JSON.stringify({ foo: "bar" }));
    expect(() => buildConfig({ ...baseEnv, ROUTES_FILE: path })).toThrow();
  });
});
