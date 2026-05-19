// src/config/schemas.ts
import { z } from "zod";

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
