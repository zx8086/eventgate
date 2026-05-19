// src/config/schemas.ts
import { z } from "zod";

export const configSchema = z
  .strictObject({
    app: z.strictObject({
      name: z.string().min(1),
      version: z.string().min(1),
      environment: z.enum(["dev", "staging", "prod", "test"]).describe("Deployment environment."),
      tenant: z.string().min(1).describe("Logical tenant; flows onto every normalized event."),
    }),
    server: z.strictObject({
      port: z.number().int().min(1).max(65535),
    }),
    kafka: z.strictObject({
      brokers: z.array(z.string().min(1)).min(1).describe("Kafka bootstrap brokers."),
      clientIdGateway: z.string().min(1),
      clientIdWriter: z.string().min(1),
      groupId: z.string().min(1),
      auth: z
        .enum(["none", "iam"])
        .describe("Kafka SASL mechanism. 'none' for local Redpanda, 'iam' for AWS MSK Serverless."),
      region: z
        .string()
        .min(1)
        .optional()
        .describe("AWS region for MSK IAM SASL token signing. Required when auth='iam'."),
      topics: z.strictObject({
        raw: z.string().min(1),
        events: z.string().min(1),
        dlq: z.string().min(1),
      }),
    }),
    couchbase: z.strictObject({
      enabled: z.boolean().describe("When false, the writer logs events only and skips Couchbase."),
      connStr: z
        .string()
        .regex(/^couchbases?:\/\/.+/, "must start with couchbase:// or couchbases://"),
      username: z.string().min(1),
      password: z.string().min(1),
      bucket: z.string().min(1),
      scope: z.string().min(1),
      historyCollection: z.string().min(1),
      stateCollection: z.string().min(1),
    }),
    observability: z.strictObject({
      logLevel: z.enum(["trace", "debug", "info", "warn", "error", "fatal", "silent"]),
    }),
  })
  .superRefine((cfg, ctx) => {
    if (cfg.kafka.auth === "iam" && !cfg.kafka.region) {
      ctx.addIssue({
        code: "custom",
        path: ["kafka", "region"],
        message: "kafka.region is required when auth=iam",
      });
    }

    if (cfg.app.environment !== "prod") return;

    if (cfg.kafka.auth !== "iam") {
      ctx.addIssue({
        code: "custom",
        path: ["kafka", "auth"],
        message: "kafka.auth=iam is required in prod",
      });
    }
    if (cfg.kafka.brokers.some((b) => b.includes("localhost"))) {
      ctx.addIssue({
        code: "custom",
        path: ["kafka", "brokers"],
        message: "localhost brokers are not allowed in prod",
      });
    }

    if (!cfg.couchbase.enabled) return;

    if (cfg.couchbase.connStr.includes("localhost")) {
      ctx.addIssue({
        code: "custom",
        path: ["couchbase", "connStr"],
        message: "localhost connection string is not allowed in prod",
      });
    }
    if (!cfg.couchbase.connStr.startsWith("couchbases://")) {
      ctx.addIssue({
        code: "custom",
        path: ["couchbase", "connStr"],
        message: "TLS (couchbases://) is required in prod",
      });
    }
    if (cfg.couchbase.password === "password") {
      ctx.addIssue({
        code: "custom",
        path: ["couchbase", "password"],
        message: "default password is not allowed in prod",
      });
    }
  });

export type AppConfig = z.infer<typeof configSchema>;
