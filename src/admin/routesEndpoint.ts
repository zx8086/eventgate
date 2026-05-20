// src/admin/routesEndpoint.ts
// PUT /admin/routes handler: token auth, Zod validation, atomic-write
// persistence, hot reload. See: docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md

import { routesSchema, type RouteConfig } from "../config/schemas.ts";
import { getLogger } from "../logging/index.ts";
import { verifyAdminToken } from "./auth.ts";
import { writeRoutesFile } from "./routesFile.ts";

const log = getLogger("admin.routesEndpoint");

export type AdminRoutesDeps = {
  expectedToken: string;
  routesFilePath: string;
  onReload: (routes: RouteConfig[]) => void | Promise<void>;
};

export function makeAdminRoutesHandler(deps: AdminRoutesDeps) {
  const { expectedToken, routesFilePath, onReload } = deps;

  return async function handler(req: Request): Promise<Response> {
    const provided = req.headers.get("x-admin-token");
    if (!verifyAdminToken(provided, expectedToken)) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }

    let body: unknown;
    try {
      body = await req.json();
    } catch {
      return Response.json({ error: "invalid JSON body" }, { status: 400 });
    }

    const parsed = routesSchema.safeParse(body);
    if (!parsed.success) {
      return Response.json(
        {
          error: "validation",
          issues: parsed.error.issues.map((i) => ({
            path: i.path,
            message: i.message,
          })),
        },
        { status: 400 },
      );
    }

    try {
      await writeRoutesFile(routesFilePath, parsed.data);
    } catch (err) {
      log.error({ err, routesFilePath }, "admin routes persist failed");
      return Response.json({ error: "persist failed" }, { status: 500 });
    }

    try {
      await onReload(parsed.data);
    } catch (err) {
      log.error({ err }, "admin routes reload failed");
      return Response.json(
        {
          error: "reload failed",
          message: "routes persisted; restart will apply",
        },
        { status: 500 },
      );
    }

    return Response.json({ routes: parsed.data }, { status: 200 });
  };
}
