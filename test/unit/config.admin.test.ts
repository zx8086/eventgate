// test/unit/config.admin.test.ts
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
  dir = mkdtempSync(join(tmpdir(), "eventgate-admin-"));
});
afterEach(() => {
  process.env = snapshot;
  if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("config.admin", () => {
  it("is undefined when ADMIN_TOKEN is unset", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.admin).toBeUndefined();
  });

  it("populates admin.token when ADMIN_TOKEN is set", () => {
    const token = "a".repeat(32);
    const cfg = buildConfig({ ...baseEnv, ADMIN_TOKEN: token });
    expect(cfg.admin?.token).toBe(token);
  });

  it("rejects ADMIN_TOKEN shorter than 32 characters", () => {
    expect(() => buildConfig({ ...baseEnv, ADMIN_TOKEN: "tooshort" })).toThrow(/Invalid configuration/);
  });
});

describe("config.routesFile", () => {
  it("is undefined when ROUTES_FILE is unset", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.routesFile).toBeUndefined();
  });

  it("is populated when ROUTES_FILE points at a valid routes array", () => {
    const path = join(dir, "routes.json");
    writeFileSync(
      path,
      JSON.stringify([
        {
          name: "test-route",
          path: "/webhooks/test",
          topic: "T_PRIVATE_SOURCE_TEST_X",
          dlqTopic: "DLQ_T_PRIVATE_SOURCE_TEST_X",
          sourceHeader: "test-route",
          keyFields: ["id"],
          idempotency: "elastic-autoops",
        },
      ]),
    );
    const cfg = buildConfig({ ...baseEnv, ROUTES_FILE: path });
    expect(cfg.routesFile).toBe(path);
  });
});
