// test/unit/config.admin.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig } from "../../src/config/loader.ts";

let snapshot: NodeJS.ProcessEnv;
const baseEnv = { ENVIRONMENT: "dev", KAFKA_PROVIDER: "local" };

beforeEach(() => { snapshot = { ...process.env }; });
afterEach(() => { process.env = snapshot; });

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

  it("is populated when ROUTES_FILE is set", () => {
    const cfg = buildConfig({ ...baseEnv, ROUTES_FILE: "/tmp/routes.json" });
    expect(cfg.routesFile).toBe("/tmp/routes.json");
  });
});
