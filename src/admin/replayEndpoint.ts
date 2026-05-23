// src/admin/replayEndpoint.ts
import { z } from "zod";
import type { ReplayConfig, RouteConfig } from "../config/schemas.ts";
import type { HealthAdmin } from "../health/admin.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { ReplayConsumer } from "../replay/consumer.ts";
import type { DlqDepthCache } from "../replay/dlqInspector.ts";
import {
  parseAttempt,
  readHeader,
  stampAuditHeaders,
  stripConnectHeaders,
} from "../replay/headers.ts";
import type { ReplayJobStore } from "../replay/jobStore.ts";
import { runReplayBatch, runReplayBatchDryRun } from "../replay/runner.ts";
import { triage } from "../replay/triage.ts";
import { verifyAdminToken } from "./auth.ts";

const log = getLogger("admin.replayEndpoint");

export type ReplayDeps = {
  expectedToken: string;
  cfg: ReplayConfig;
  jobStore: ReplayJobStore;
  routes: ReadonlyMap<string, RouteConfig>;
  producer: EventProducer;
  admin: HealthAdmin;
  createConsumer: (route: RouteConfig, jobId: string) => Promise<ReplayConsumer>;
  dlqDepth?: DlqDepthCache;
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
  const { expectedToken, cfg, jobStore, routes, producer, createConsumer, dlqDepth } = deps;

  return {
    listDlq: async (req) => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) {
        return unauthorized();
      }
      const out = [...routes.values()].map((r) => ({
        route: r.name,
        dlqTopic: r.dlqTopic,
        partitions: dlqDepth?.get(r.name) ?? [],
        lastJob: jobStore.lastJobForRoute?.(r.name) ?? null,
      }));
      return Response.json({ routes: out });
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

            if (parsed.data.dryRun) {
              const result = await runReplayBatchDryRun({
                records: stream,
                producer,
              });
              jobStore.update(job.id, {
                ...result,
                status: "done",
                finishedAt: Date.now(),
              });
            } else {
              const result = await runReplayBatch({
                records: stream,
                producer,
                cfg,
                jobId: job.id,
                route,
                signal: ctl.signal,
                onProgress: (snapshot) => jobStore.update(job.id, snapshot),
              });
              const terminalStatus = result.paused ? "paused" : "done";
              jobStore.update(job.id, {
                scanned: result.scanned,
                replayed: result.replayed,
                parked: result.parked,
                skipped: result.skipped,
                errors: result.errors,
                lastOffset: result.lastOffset,
                status: terminalStatus,
                lastError: result.lastError,
                nextResumeAt: result.nextResumeAt,
                finishedAt: Date.now(),
              });
            }
          } finally {
            await consumer.close();
          }
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          log.warn({ jobId: job.id, err: message }, "bulk run failed");
          jobStore.update(job.id, {
            status: "failed",
            lastError: message,
            finishedAt: Date.now(),
          });
        }
      })();

      return Response.json(
        { jobId: job.id, status: "running", dryRun: parsed.data.dryRun },
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
          const decision = triage(rec, cfg);

          // Build the headers we WOULD send so the response is always
          // informative — in dry-run we never produce; in real mode we send
          // these exact headers via the producer.
          const targetTopic =
            decision.kind === "replay"
              ? route.topic
              : cfg.parkingTopicSuffix === ""
                ? null
                : route.topic + cfg.parkingTopicSuffix;
          // Park records do NOT bump the attempt counter (spec §"On park"):
          // it represents how many times we have tried to *replay* this
          // record into the source topic. Parking is a terminal classification
          // for this attempt, not a re-attempt.
          const nextAttempt =
            decision.kind === "replay" ? attempt + 1 : attempt;
          const headersOut = stampAuditHeaders(
            stripConnectHeaders(rec.headers),
            {
              jobId: job.id,
              sourceTopic: rec.topic,
              partition: rec.partition,
              offset: rec.offset,
              attempt: nextAttempt,
            },
          );

          if (parsed.data.dryRun) {
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
                targetTopic,
                wouldStampHeaders: headersOut.map(([k, v]) => ({
                  name: k?.toString("utf-8") ?? null,
                  value: v?.toString("utf-8") ?? null,
                })),
              },
              { status: 200 },
            );
          }

          // Real non-dry-run path: produce to the target topic.
          if (targetTopic === null) {
            // Park decision with empty parkingTopicSuffix => count-only.
            jobStore.update(job.id, {
              status: "done",
              scanned: 1,
              parked: 1,
              finishedAt: Date.now(),
            });
            return Response.json(
              { decision, replayed: false, parked: true, dryRun: false },
              { status: 200 },
            );
          }

          // Producer requires string key/value. The DlqRecord carries raw
          // Buffer key/value; we pass them through utf-8 because the gateway
          // only ever produces utf-8 keys/values (string serializers).
          const keyStr = rec.key === null ? "" : rec.key.toString("utf-8");
          const valueStr = rec.value === null ? "" : rec.value.toString("utf-8");
          // Convert raw HeaderTuple array to the array-of-tuples form
          // ProducerHeaders accepts. Null keys are skipped (spec: a missing
          // key is not addressable Kafka-side and the SDK rejects).
          const headersArr: Array<[string, string | Buffer | null]> = [];
          for (const [k, v] of headersOut) {
            if (k === null) continue;
            headersArr.push([k.toString("utf-8"), v]);
          }

          await producer.sendByTopic(targetTopic, keyStr, valueStr, headersArr);
          if (decision.kind === "replay") {
            jobStore.update(job.id, {
              status: "done",
              scanned: 1,
              replayed: 1,
              finishedAt: Date.now(),
            });
            return Response.json(
              { decision, replayed: true, parked: false, dryRun: false, targetTopic },
              { status: 200 },
            );
          }
          jobStore.update(job.id, {
            status: "done",
            scanned: 1,
            parked: 1,
            finishedAt: Date.now(),
          });
          return Response.json(
            { decision, replayed: false, parked: true, dryRun: false, targetTopic },
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
