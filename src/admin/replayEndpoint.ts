// src/admin/replayEndpoint.ts
import { z } from "zod";
import type { RouteConfig } from "../config/schemas.ts";
import type { HealthAdmin } from "../health/admin.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { ReplayConsumer } from "../replay/consumer.ts";
import {
  parseAttempt,
  readHeader,
  stampAuditHeaders,
  stripConnectHeaders,
} from "../replay/headers.ts";
import type { ReplayJobStore } from "../replay/jobStore.ts";
import { runReplayBatchDryRun } from "../replay/runner.ts";
import { verifyAdminToken } from "./auth.ts";

const log = getLogger("admin.replayEndpoint");

export type ReplayDeps = {
  expectedToken: string;
  jobStore: ReplayJobStore;
  routes: ReadonlyMap<string, RouteConfig>;
  producer: EventProducer;
  admin: HealthAdmin;
  createConsumer: (route: RouteConfig, jobId: string) => Promise<ReplayConsumer>;
};

const singleBodySchema = z.strictObject({
  partition: z.number().int().nonnegative(),
  offset: z.number().int().nonnegative(),
  dryRun: z.boolean().default(true),
});

const bulkBodySchema = z.strictObject({
  partition: z.number().int().nonnegative(),
  dryRun: z.boolean().default(true),
  fromOffset: z.number().int().nonnegative().optional(),
  toOffset: z.number().int().nonnegative().optional(),
  maxRecords: z.number().int().positive().optional(),
  filter: z
    .strictObject({ exceptionClass: z.string().min(1).optional() })
    .optional(),
});

const DEFAULT_MAX_RECORDS = 10_000;

function unauthorized(): Response {
  return Response.json({ error: "unauthorized" }, { status: 401 });
}

// pathParam extracts a single segment from req.url. We use this instead of
// req.params because Bun.serve's :param routing types aren't stable across
// runtime versions; URL-based parsing is portable and predictable. When suffix
// is provided, the path must end with it and is stripped before return.
function pathParam(
  req: Request,
  prefix: string,
  suffix?: string,
): string | null {
  const url = new URL(req.url);
  let path = url.pathname;
  if (!path.startsWith(prefix)) return null;
  path = path.slice(prefix.length);
  if (suffix !== undefined) {
    if (!path.endsWith(suffix)) return null;
    path = path.slice(0, path.length - suffix.length);
  }
  return path.length === 0 ? null : path;
}

export type ReplayHandlers = {
  listDlq: (req: Request) => Promise<Response>;
  bulkReplay: (req: Request) => Promise<Response>;
  singleReplay: (req: Request) => Promise<Response>;
  jobStatus: (req: Request) => Promise<Response>;
  cancelJob: (req: Request) => Promise<Response>;
};

export function makeReplayHandlers(deps: ReplayDeps): ReplayHandlers {
  const { expectedToken, jobStore, routes, producer, createConsumer } = deps;

  return {
    // Phase 3 will populate per-partition depth via Admin.listOffsets + cache;
    // Phase 1 returns an empty partitions array so operators have a stable
    // shape to script against.
    listDlq: async (req) => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) {
        return unauthorized();
      }
      return Response.json({
        routes: [...routes.values()].map((r) => ({
          route: r.name,
          dlqTopic: r.dlqTopic,
          partitions: [],
          lastJob: null,
        })),
      });
    },

    bulkReplay: async (req) => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) {
        return unauthorized();
      }
      const routeName = pathParam(req, "/admin/replay/");
      if (routeName === null) {
        return Response.json({ error: "missing route" }, { status: 400 });
      }
      const route = routes.get(routeName);
      if (route === undefined) {
        return Response.json({ error: "unknown route" }, { status: 404 });
      }

      let body: unknown;
      try {
        body = await req.json();
      } catch {
        return Response.json({ error: "invalid JSON body" }, { status: 400 });
      }
      const parsed = bulkBodySchema.safeParse(body);
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

      const job = jobStore.create({
        route: route.name,
        partition: parsed.data.partition,
        mode: "manual",
        dryRun: parsed.data.dryRun,
        fromOffset: parsed.data.fromOffset,
        toOffset: parsed.data.toOffset,
      });

      // Non-dry-run is accepted but deferred to Phase 4 (real bulk runner with
      // triage + rate limit + parking). For Phase 1 we mark the job failed
      // immediately with a clear lastError so the operator knows what happened.
      if (!parsed.data.dryRun) {
        jobStore.update(job.id, {
          status: "failed",
          lastError: "bulk_non_dryrun_not_implemented_yet",
          finishedAt: Date.now(),
        });
        return Response.json(
          {
            jobId: job.id,
            status: "failed",
            dryRun: false,
            message: "bulk replay (non-dry-run) ships in a later phase",
          },
          { status: 202 },
        );
      }

      // Dry-run runs asynchronously so the HTTP response is non-blocking.
      const ctl = new AbortController();
      jobStore.setCancelHandle(job.id, ctl);
      jobStore.update(job.id, { status: "running" });

      void (async () => {
        try {
          const consumer = await createConsumer(route, job.id);
          try {
            const stream = consumer.streamRange({
              partition: parsed.data.partition,
              fromOffset: parsed.data.fromOffset ?? 0,
              toOffset: parsed.data.toOffset,
              maxRecords: parsed.data.maxRecords ?? DEFAULT_MAX_RECORDS,
              signal: ctl.signal,
            });
            const result = await runReplayBatchDryRun({
              records: stream,
              producer,
            });
            jobStore.update(job.id, {
              ...result,
              status: "done",
              finishedAt: Date.now(),
            });
          } finally {
            await consumer.close();
          }
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          log.warn({ jobId: job.id, err: message }, "bulk dry-run failed");
          jobStore.update(job.id, {
            status: "failed",
            lastError: message,
            finishedAt: Date.now(),
          });
        }
      })();

      return Response.json(
        { jobId: job.id, status: "running", dryRun: true },
        { status: 202 },
      );
    },

    singleReplay: async (req) => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) {
        return unauthorized();
      }
      const routeName = pathParam(req, "/admin/replay/", "/message");
      if (routeName === null) {
        return Response.json({ error: "missing route" }, { status: 400 });
      }
      const route = routes.get(routeName);
      if (route === undefined) {
        return Response.json({ error: "unknown route" }, { status: 404 });
      }

      let body: unknown;
      try {
        body = await req.json();
      } catch {
        return Response.json({ error: "invalid JSON body" }, { status: 400 });
      }
      const parsed = singleBodySchema.safeParse(body);
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

      // Phase 1 stub: triage always returns "replay" so the operator sees what
      // the headers WOULD look like after re-produce. Phase 2 swaps this for
      // the real triage() function with transient/poison classification.
      const job = jobStore.create({
        route: route.name,
        partition: parsed.data.partition,
        mode: "single",
        dryRun: parsed.data.dryRun,
        fromOffset: parsed.data.offset,
        toOffset: parsed.data.offset,
      });

      try {
        const consumer = await createConsumer(route, job.id);
        try {
          const rec = await consumer.fetchOne({
            partition: parsed.data.partition,
            offset: parsed.data.offset,
          });
          if (rec === null) {
            jobStore.update(job.id, {
              status: "done",
              scanned: 0,
              finishedAt: Date.now(),
            });
            return Response.json(
              {
                decision: null,
                replayed: false,
                parked: false,
                message: "record not found at the requested offset",
              },
              { status: 200 },
            );
          }

          const attempt = parseAttempt(
            readHeader(rec, "x-eventgate-replay-attempt"),
          );
          const exceptionClass =
            readHeader(rec, "__connect.errors.exception.class.name") ?? null;
          const decision = { kind: "replay" as const, exceptionClass };

          // Non-dry-run single is deferred until Phase 2 ships real triage —
          // we should not actually re-produce without the classification logic
          // in place.
          if (!parsed.data.dryRun) {
            jobStore.update(job.id, {
              status: "failed",
              lastError: "single_non_dryrun_not_implemented_yet",
              finishedAt: Date.now(),
            });
            return Response.json(
              {
                decision,
                replayed: false,
                parked: false,
                message:
                  "non-dry-run single-message replay ships in a later phase",
              },
              { status: 202 },
            );
          }

          // Build the headers we WOULD send so the response is informative.
          // No actual produce in dry-run.
          const wouldSend = stampAuditHeaders(stripConnectHeaders(rec.headers), {
            jobId: job.id,
            sourceTopic: rec.topic,
            partition: rec.partition,
            offset: rec.offset,
            attempt: attempt + 1,
          });

          jobStore.update(job.id, {
            status: "done",
            scanned: 1,
            finishedAt: Date.now(),
          });
          return Response.json(
            {
              decision,
              replayed: false,
              parked: false,
              dryRun: true,
              wouldStampHeaders: wouldSend.map(([k, v]) => ({
                name: k?.toString("utf-8") ?? null,
                value: v?.toString("utf-8") ?? null,
              })),
            },
            { status: 200 },
          );
        } finally {
          await consumer.close();
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ jobId: job.id, err: message }, "single dry-run failed");
        jobStore.update(job.id, {
          status: "failed",
          lastError: message,
          finishedAt: Date.now(),
        });
        return Response.json(
          { error: "consumer failure", message },
          { status: 500 },
        );
      }
    },

    jobStatus: async (req) => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) {
        return unauthorized();
      }
      const id = pathParam(req, "/admin/replay/");
      if (id === null) {
        return Response.json({ error: "missing jobId" }, { status: 400 });
      }
      const job = jobStore.get(id);
      if (job === null) {
        return Response.json({ error: "unknown job" }, { status: 404 });
      }
      return Response.json(job);
    },

    cancelJob: async (req) => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) {
        return unauthorized();
      }
      const id = pathParam(req, "/admin/replay/", "/cancel");
      if (id === null) {
        return Response.json({ error: "missing jobId" }, { status: 400 });
      }
      const cancelled = jobStore.cancel(id);
      if (!cancelled && jobStore.get(id) === null) {
        return Response.json({ error: "unknown job" }, { status: 404 });
      }
      return Response.json({ jobId: id, cancelled });
    },
  };
}
