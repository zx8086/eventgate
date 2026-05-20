// src/config/schemas.ts
import { z } from "zod";
import { checkGatewayTopic, expectedDlqTopic } from "./topicPolicy.ts";
import { checkReservedPath } from "./reservedPaths.ts";
import { knownIdempotencyStrategy } from "../gateway/idempotencyStrategies.ts";

const localSchema = z.strictObject({
  bootstrapServers: z
    .array(z.string().min(1))
    .min(1)
    .describe("Bootstrap brokers for local Kafka / Redpanda."),
});

const mskSchema = z.strictObject({
  region: z
    .string()
    .describe("AWS region of the MSK cluster. Required when provider=msk."),
  clusterArn: z
    .string()
    .describe("MSK cluster ARN. Either this or msk.brokers must be set."),
  brokers: z
    .string()
    .describe("CSV bootstrap brokers. Either this or msk.clusterArn must be set."),
  authMode: z
    .enum(["iam", "tls", "none"])
    .describe("MSK auth mode. 'iam' uses OAUTHBEARER SASL with a short-lived IAM token."),
});

const confluentSchema = z.strictObject({
  bootstrapServers: z.string().describe("Confluent Cloud bootstrap servers (host:port)."),
  apiKey: z.string().describe("Confluent Cloud API key (SASL/PLAIN username)."),
  apiSecret: z.string().describe("Confluent Cloud API secret (SASL/PLAIN password)."),
});

const routeSchema = z.strictObject({
  name: z.string().min(1).describe("Human-readable route id; used for logs and as default sourceHeader."),
  path: z
    .string()
    .min(2)
    .startsWith("/")
    .describe("Literal HTTP path the gateway listens on, e.g. /webhooks/elastic/autoops."),
  topic: z
    .string()
    .min(1)
    .describe("Full Kafka topic name; must match T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>."),
  dlqTopic: z
    .string()
    .min(1)
    .optional()
    .describe("Optional companion DLQ name. If set, must equal DLQ_T_<topic>. Gateway never writes here."),
  sourceHeader: z
    .string()
    .min(1)
    .optional()
    .describe("Override for the 'source' Kafka header. Defaults to name."),
  keyFields: z
    .array(z.string().min(1))
    .min(1)
    .describe("Body fields to consult in order for the partition key. First non-empty string wins."),
  idempotency: z
    .string()
    .min(1)
    .optional()
    .describe("Named strategy from idempotencyStrategies registry. Optional."),
});

export type RouteConfig = z.infer<typeof routeSchema>;

export const routesSchema = z
  .array(routeSchema)
  .min(1, "at least one route is required")
  .superRefine((routes, ctx) => {
    const pathSeen = new Map<string, number>();
    const topicSeen = new Map<string, number>();
    const dlqSeen = new Map<string, number>();

    routes.forEach((r, i) => {
      const topicCheck = checkGatewayTopic(r.topic);
      if (!topicCheck.ok) {
        ctx.addIssue({
          code: "custom",
          path: [i, "topic"],
          message: topicCheck.message,
        });
      }

      const reservedCheck = checkReservedPath(r.path);
      if (!reservedCheck.ok) {
        ctx.addIssue({
          code: "custom",
          path: [i, "path"],
          message: reservedCheck.message,
        });
      }

      if (r.dlqTopic !== undefined) {
        const expected = expectedDlqTopic(r.topic);
        if (r.dlqTopic !== expected) {
          ctx.addIssue({
            code: "custom",
            path: [i, "dlqTopic"],
            message: `dlqTopic must be '${expected}'; got '${r.dlqTopic}'`,
          });
        }
        if (r.dlqTopic.length > 249) {
          ctx.addIssue({
            code: "custom",
            path: [i, "dlqTopic"],
            message: `dlqTopic length ${r.dlqTopic.length} exceeds Kafka limit of 249`,
          });
        }
      }

      if (r.idempotency !== undefined && !knownIdempotencyStrategy(r.idempotency)) {
        ctx.addIssue({
          code: "custom",
          path: [i, "idempotency"],
          message: `unknown idempotency strategy '${r.idempotency}'`,
        });
      }

      const prevPath = pathSeen.get(r.path);
      if (prevPath !== undefined) {
        ctx.addIssue({
          code: "custom",
          path: [i, "path"],
          message: `duplicate path '${r.path}' (also at routes[${prevPath}])`,
        });
      } else {
        pathSeen.set(r.path, i);
      }

      const prevTopic = topicSeen.get(r.topic);
      if (prevTopic !== undefined) {
        ctx.addIssue({
          code: "custom",
          path: [i, "topic"],
          message: `duplicate topic '${r.topic}' (also at routes[${prevTopic}])`,
        });
      } else {
        topicSeen.set(r.topic, i);
      }

      if (r.dlqTopic !== undefined) {
        const prevDlq = dlqSeen.get(r.dlqTopic);
        if (prevDlq !== undefined) {
          ctx.addIssue({
            code: "custom",
            path: [i, "dlqTopic"],
            message: `duplicate dlqTopic '${r.dlqTopic}' (also at routes[${prevDlq}])`,
          });
        } else {
          dlqSeen.set(r.dlqTopic, i);
        }
      }
    });
  });

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
      provider: z
        .enum(["local", "msk", "confluent"])
        .describe("Which Kafka backend to connect to. Selected by KAFKA_PROVIDER."),
      clientId: z.string().min(1).describe("Kafka client id used by the producer."),
      topics: z.strictObject({
        raw: z.string().min(1),
        events: z.string().min(1),
        dlq: z.string().min(1),
      }),
      local: localSchema,
      msk: mskSchema,
      confluent: confluentSchema,
    }),
    observability: z.strictObject({
      logLevel: z.enum(["trace", "debug", "info", "warn", "error", "fatal", "silent"]),
    }),
    outbox: z.strictObject({
      enabled: z
        .boolean()
        .describe("Whether to use the SQLite outbox. When false, publishes inline (escape hatch)."),
      dbPath: z
        .string()
        .min(1)
        .describe("SQLite database file path. Use ':memory:' for tests."),
      batchSize: z
        .number()
        .int()
        .positive()
        .describe("Maximum rows the drainer fetches per loop iteration."),
      backoffMaxMs: z
        .number()
        .int()
        .positive()
        .describe("Cap on exponential backoff delay between retries, in ms."),
      maxAgeHours: z
        .number()
        .int()
        .positive()
        .describe("After this age, a stuck row is marked 'failed' and surfaced via /healthz."),
      idlePollMs: z
        .number()
        .int()
        .positive()
        .describe("Drainer poll interval when the previous batch was empty."),
      busyPollMs: z
        .number()
        .int()
        .positive()
        .describe("Drainer poll interval when the previous batch was full."),
      backlogWarnThreshold: z
        .number()
        .int()
        .positive()
        .describe("Pending-row count above which the gateway logs a warn each iteration."),
    }),
    routes: routesSchema,
  })
  .superRefine((cfg, ctx) => {
    const { kafka, app } = cfg;

    if (kafka.provider === "msk") {
      if (!kafka.msk.region) {
        ctx.addIssue({
          code: "custom",
          path: ["kafka", "msk", "region"],
          message: "msk.region is required when provider=msk",
        });
      }
      if (!kafka.msk.clusterArn && !kafka.msk.brokers) {
        ctx.addIssue({
          code: "custom",
          path: ["kafka", "msk"],
          message: "msk requires either clusterArn or brokers when provider=msk",
        });
      }
    }

    if (kafka.provider === "confluent") {
      for (const field of ["bootstrapServers", "apiKey", "apiSecret"] as const) {
        if (!kafka.confluent[field]) {
          ctx.addIssue({
            code: "custom",
            path: ["kafka", "confluent", field],
            message: `confluent.${field} is required when provider=confluent`,
          });
        }
      }
    }

    if (app.environment === "prod" && kafka.provider === "local") {
      ctx.addIssue({
        code: "custom",
        path: ["kafka", "provider"],
        message: "provider=local is not allowed in prod; use msk or confluent",
      });
    }
  });

export type AppConfig = z.infer<typeof configSchema>;
