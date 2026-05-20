// test/unit/admin.routesEndpoint.test.ts
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeAdminRoutesHandler } from "../../src/admin/routesEndpoint.ts";

const TOKEN = "a".repeat(32);
let dir: string;
let routesFilePath: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "eventgate-admin-"));
  routesFilePath = join(dir, "routes.json");
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

const validRoutes = [
  {
    name: "datadog",
    path: "/webhooks/datadog",
    topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
    keyFields: ["alert_id"],
  },
];

function req(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request("http://localhost/admin/routes", {
    method: "PUT",
    headers: { "content-type": "application/json", ...headers },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

describe("makeAdminRoutesHandler", () => {
  it("rejects with 401 when X-Admin-Token is missing", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req(validRoutes));
    expect(res.status).toBe(401);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 401 when X-Admin-Token is wrong", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req(validRoutes, { "x-admin-token": "wrong" }));
    expect(res.status).toBe(401);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 400 when the body is not JSON", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req("not json", { "x-admin-token": TOKEN }));
    expect(res.status).toBe(400);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 400 when the body fails routesSchema (forbidden topic)", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const bad = [{ name: "x", path: "/x", topic: "T_PRIVATE_SINK_X_Y", keyFields: ["id"] }];
    const res = await handler(req(bad, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(400);
    const body = await res.json() as { issues: unknown[] };
    expect(body.issues.length).toBeGreaterThan(0);
    expect(reload).not.toHaveBeenCalled();
  });

  it("rejects with 400 when the body declares a reserved path", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const bad = [{ name: "x", path: "/healthz", topic: "T_PRIVATE_SOURCE_X_Y", keyFields: ["id"] }];
    const res = await handler(req(bad, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(400);
    expect(reload).not.toHaveBeenCalled();
  });

  it("persists, reloads, and returns 200 on success", async () => {
    const reload = mock((routes: unknown[]) => { void routes; });
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: reload,
    });
    const res = await handler(req(validRoutes, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(200);
    const fileContents = JSON.parse(readFileSync(routesFilePath, "utf8"));
    expect(fileContents[0].name).toBe("datadog");
    expect(reload).toHaveBeenCalledTimes(1);
    const reloadArg = reload.mock.calls[0]?.[0] as { name: string }[];
    expect(reloadArg[0]?.name).toBe("datadog");
    const body = await res.json() as { routes: { name: string }[] };
    expect(body.routes[0]?.name).toBe("datadog");
  });

  it("returns 500 if persistence fails (read-only dir)", async () => {
    const reload = mock(() => {});
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath: "/nonexistent/dir/routes.json",
      onReload: reload,
    });
    const res = await handler(req(validRoutes, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(500);
    expect(reload).not.toHaveBeenCalled();
  });

  it("returns 500 with distinguishable message when onReload throws synchronously", async () => {
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: () => {
        throw new Error("reload boom");
      },
    });
    const res = await handler(req(validRoutes, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(500);
    const body = await res.json() as { error: string; message: string };
    expect(body.error).toBe("reload failed");
    expect(body.message).toBe("routes persisted; restart will apply");
  });

  it("returns 500 when onReload rejects asynchronously", async () => {
    const handler = makeAdminRoutesHandler({
      expectedToken: TOKEN,
      routesFilePath,
      onReload: async () => {
        throw new Error("async reload boom");
      },
    });
    const res = await handler(req(validRoutes, { "x-admin-token": TOKEN }));
    expect(res.status).toBe(500);
    const body = await res.json() as { error: string; message: string };
    expect(body.error).toBe("reload failed");
    expect(body.message).toBe("routes persisted; restart will apply");
  });
});
