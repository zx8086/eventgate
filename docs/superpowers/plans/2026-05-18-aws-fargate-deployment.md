# AWS Fargate Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the eventgate gateway and writer to AWS ECS Fargate in `eu-central-1`, with MSK Serverless as the Kafka backend, exposed on a public ALB. Writer logs normalized events to CloudWatch (Couchbase deferred).

**Architecture:** Two services in one VPC. Gateway runs Bun.serve behind a public ALB. Writer runs a long-running kafkajs consumer. Both authenticate to MSK Serverless via IAM SASL. Single Docker image, two task definitions.

**Tech Stack:** Bun 1.x, kafkajs, `aws-msk-iam-sasl-signer-js`, Docker, AWS ECS Fargate, AWS MSK Serverless, AWS ALB, AWS ECR, AWS CloudWatch Logs, AWS CLI (via AWS MCP `call_aws`).

**Linear:** [SIO-789](https://linear.app/siobytes/issue/SIO-789/deploy-eventgate-to-aws-fargate-v1-eu-central-1-no-couchbase)

**Spec:** [docs/superpowers/specs/2026-05-18-aws-fargate-deployment-design.md](../specs/2026-05-18-aws-fargate-deployment-design.md)

---

## File map

**New files:**
- `Dockerfile` — multi-stage Bun image
- `.dockerignore`
- `scripts/deploy/lib.sh` — shared bash helpers (region, error handling, `.env.aws` read/write)
- `scripts/deploy/01-network.sh`
- `scripts/deploy/02-security-groups.sh`
- `scripts/deploy/03-msk.sh`
- `scripts/deploy/04-ecr.sh`
- `scripts/deploy/05-ecs-cluster.sh`
- `scripts/deploy/06-log-groups.sh`
- `scripts/deploy/07-alb.sh`
- `scripts/deploy/08-iam-roles.sh`
- `scripts/deploy/09-create-topics.ts` — Bun script using kafkajs admin
- `scripts/deploy/10-register-task-defs.sh`
- `scripts/deploy/11-deploy-services.sh`
- `scripts/deploy/12-print-url.sh`
- `scripts/deploy/teardown.sh`
- `scripts/deploy/README.md`
- `test/unit/config.iam-auth.test.ts` — superRefine rules for IAM SASL + couchbase guard

**Modified files:**
- `src/config/schemas.ts` — add `kafka.auth`, `kafka.region`, `couchbase.enabled`, new superRefine rules
- `src/config/defaults.ts` — defaults for the new fields
- `src/config/envMapping.ts` — wire `KAFKA_AUTH`, `KAFKA_REGION`, `COUCHBASE_ENABLED`
- `src/kafka/producer.ts` — IAM SASL support
- `src/kafka/consumer.ts` — IAM SASL support
- `src/writer/index.ts` — `couchbase.enabled` guard, log-only mode
- `package.json` — add `aws-msk-iam-sasl-signer-js` dep
- `.env.example` — document new env vars
- `.gitignore` — add `scripts/deploy/.env.aws`

---

## Task 1: Add Kafka auth + Couchbase enabled to config schema

**Files:**
- Modify: `src/config/schemas.ts`
- Modify: `src/config/defaults.ts`
- Modify: `src/config/envMapping.ts`
- Create: `test/unit/config.iam-auth.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `test/unit/config.iam-auth.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig } from "../../src/config/loader.ts";
import { resetConfigCache } from "../../src/config/loader.ts";

const baseProdEnv = {
  ENVIRONMENT: "prod",
  COUCHBASE_ENABLED: "false",
  KAFKA_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
  KAFKA_AUTH: "iam",
  KAFKA_REGION: "eu-central-1",
};

describe("IAM SASL config", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("accepts kafka.auth=iam with a region", () => {
    const cfg = buildConfig(baseProdEnv);
    expect(cfg.kafka.auth).toBe("iam");
    expect(cfg.kafka.region).toBe("eu-central-1");
  });

  it("rejects kafka.auth=iam without a region", () => {
    const env = { ...baseProdEnv, KAFKA_REGION: "" };
    expect(() => buildConfig(env)).toThrow(/kafka\.region.*required when auth=iam/);
  });

  it("rejects prod with kafka.auth=none", () => {
    const env = { ...baseProdEnv, KAFKA_AUTH: "none" };
    expect(() => buildConfig(env)).toThrow(/iam.*required in prod/);
  });

  it("defaults kafka.auth to none for dev", () => {
    const cfg = buildConfig({});
    expect(cfg.kafka.auth).toBe("none");
  });
});

describe("Couchbase enabled gate", () => {
  beforeEach(() => resetConfigCache());
  afterEach(() => resetConfigCache());

  it("defaults couchbase.enabled to true", () => {
    const cfg = buildConfig({});
    expect(cfg.couchbase.enabled).toBe(true);
  });

  it("skips couchbase prod-safety refinements when enabled=false", () => {
    const env = {
      ENVIRONMENT: "prod",
      COUCHBASE_ENABLED: "false",
      KAFKA_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
      KAFKA_AUTH: "iam",
      KAFKA_REGION: "eu-central-1",
      COUCHBASE_CONNSTR: "couchbase://localhost",
      COUCHBASE_PASSWORD: "password",
    };
    const cfg = buildConfig(env);
    expect(cfg.couchbase.enabled).toBe(false);
  });

  it("enforces couchbase prod-safety refinements when enabled=true", () => {
    const env = {
      ENVIRONMENT: "prod",
      COUCHBASE_ENABLED: "true",
      KAFKA_BROKERS: "b-1.example.kafka-serverless.eu-central-1.amazonaws.com:9098",
      KAFKA_AUTH: "iam",
      KAFKA_REGION: "eu-central-1",
      COUCHBASE_CONNSTR: "couchbase://localhost",
      COUCHBASE_PASSWORD: "password",
    };
    expect(() => buildConfig(env)).toThrow(/localhost connection string is not allowed in prod/);
  });
});
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `bun test test/unit/config.iam-auth.test.ts`
Expected: All tests fail because `kafka.auth`, `kafka.region`, and `couchbase.enabled` are not yet in the schema.

- [ ] **Step 3: Add fields to defaults**

Replace `src/config/defaults.ts` with:

```ts
// src/config/defaults.ts
import pkg from "../../package.json" with { type: "json" };

export const defaults = {
  app: {
    name: pkg.name,
    version: pkg.version,
    environment: "dev",
    tenant: "elastic-cloud",
  },
  server: {
    port: 3000,
  },
  kafka: {
    brokers: ["localhost:9092"],
    clientIdGateway: "eventgate-gateway",
    clientIdWriter: "eventgate-writer",
    groupId: "autoops-couchbase-writer-v1",
    auth: "none" as const,
    region: undefined as string | undefined,
    topics: {
      raw: "ops.elastic.autoops.raw.v1",
      events: "ops.elastic.autoops.events.v1",
      dlq: "ops.elastic.autoops.dlq.v1",
    },
  },
  couchbase: {
    enabled: true,
    connStr: "couchbase://localhost",
    username: "Administrator",
    password: "password",
    bucket: "ops",
    scope: "_default",
    historyCollection: "autoops_events",
    stateCollection: "autoops_state",
  },
  observability: {
    logLevel: "info" as const,
  },
} as const;

export type Defaults = typeof defaults;
```

- [ ] **Step 4: Add fields and refinements to the schema**

Replace `src/config/schemas.ts` with:

```ts
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
```

- [ ] **Step 5: Wire env vars**

Replace `src/config/envMapping.ts` with:

```ts
// src/config/envMapping.ts
type RawEnv = NodeJS.ProcessEnv;

export type EnvOverrides = {
  app?: { environment?: string; tenant?: string };
  server?: { port?: number };
  kafka?: {
    brokers?: string[];
    clientIdGateway?: string;
    clientIdWriter?: string;
    groupId?: string;
    auth?: string;
    region?: string;
    topics?: { raw?: string; events?: string; dlq?: string };
  };
  couchbase?: {
    enabled?: boolean;
    connStr?: string;
    username?: string;
    password?: string;
    bucket?: string;
    scope?: string;
    historyCollection?: string;
    stateCollection?: string;
  };
  observability?: { logLevel?: string };
};

function str(v: string | undefined): string | undefined {
  return v !== undefined && v !== "" ? v : undefined;
}

function num(v: string | undefined): number | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  const n = Number(s);
  return Number.isFinite(n) ? n : undefined;
}

function bool(v: string | undefined): boolean | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  if (s.toLowerCase() === "true") return true;
  if (s.toLowerCase() === "false") return false;
  return undefined;
}

function csv(v: string | undefined): string[] | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  const parts = s.split(",").map((p) => p.trim()).filter(Boolean);
  return parts.length > 0 ? parts : undefined;
}

function filterUndefined<T extends object>(obj: T): Partial<T> {
  const out: Partial<T> = {};
  for (const key of Object.keys(obj) as (keyof T)[]) {
    if (obj[key] !== undefined) out[key] = obj[key];
  }
  return out;
}

export function mapEnv(env: RawEnv): EnvOverrides {
  const overrides: EnvOverrides = {
    app: filterUndefined({
      environment: str(env.ENVIRONMENT),
      tenant: str(env.TENANT),
    }),
    server: filterUndefined({
      port: num(env.PORT),
    }),
    kafka: filterUndefined({
      brokers: csv(env.KAFKA_BROKERS),
      clientIdGateway: str(env.KAFKA_CLIENT_ID_GATEWAY),
      clientIdWriter: str(env.KAFKA_CLIENT_ID_WRITER),
      groupId: str(env.KAFKA_GROUP_ID),
      auth: str(env.KAFKA_AUTH),
      region: str(env.KAFKA_REGION),
      topics: filterUndefined({
        raw: str(env.KAFKA_TOPIC_RAW),
        events: str(env.KAFKA_TOPIC_EVENTS),
        dlq: str(env.KAFKA_TOPIC_DLQ),
      }),
    }),
    couchbase: filterUndefined({
      enabled: bool(env.COUCHBASE_ENABLED),
      connStr: str(env.COUCHBASE_CONNSTR),
      username: str(env.COUCHBASE_USERNAME),
      password: str(env.COUCHBASE_PASSWORD),
      bucket: str(env.COUCHBASE_BUCKET),
      scope: str(env.COUCHBASE_SCOPE),
      historyCollection: str(env.COUCHBASE_HISTORY_COLLECTION),
      stateCollection: str(env.COUCHBASE_STATE_COLLECTION),
    }),
    observability: filterUndefined({
      logLevel: str(env.LOG_LEVEL),
    }),
  };

  for (const k of Object.keys(overrides) as (keyof EnvOverrides)[]) {
    const section = overrides[k];
    if (section && typeof section === "object" && Object.keys(section).length === 0) {
      delete overrides[k];
    }
  }
  return overrides;
}
```

- [ ] **Step 6: Run all tests and typecheck**

Run: `bun run typecheck && bun test`
Expected: All tests pass, including the new `config.iam-auth.test.ts`.

- [ ] **Step 7: Commit**

```bash
git add src/config/schemas.ts src/config/defaults.ts src/config/envMapping.ts test/unit/config.iam-auth.test.ts
git commit -m "SIO-789: add KAFKA_AUTH=iam and COUCHBASE_ENABLED config"
```

---

## Task 2: Add IAM SASL to Kafka producer and consumer

**Files:**
- Modify: `src/kafka/producer.ts`
- Modify: `src/kafka/consumer.ts`
- Modify: `package.json`

- [ ] **Step 1: Add the MSK IAM SASL signer dependency**

Run: `bun add aws-msk-iam-sasl-signer-js`
Expected: `package.json` and `bun.lock` updated, no errors.

- [ ] **Step 2: Create a shared kafkajs config builder**

Create `src/kafka/clientConfig.ts`:

```ts
// src/kafka/clientConfig.ts
import { generateAuthToken } from "aws-msk-iam-sasl-signer-js";
import { type KafkaConfig, logLevel } from "kafkajs";
import { config } from "../config/index.ts";

export function buildKafkaConfig(clientId: string): KafkaConfig {
  const base: KafkaConfig = {
    clientId,
    brokers: config.kafka.brokers,
    logLevel: logLevel.INFO,
  };

  if (config.kafka.auth === "iam") {
    const region = config.kafka.region;
    if (!region) throw new Error("kafka.region is required when auth=iam");
    return {
      ...base,
      ssl: true,
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
          const { token } = await generateAuthToken({ region });
          return { value: token };
        },
      },
    };
  }

  return base;
}
```

- [ ] **Step 3: Update the producer to use the shared config**

Replace `src/kafka/producer.ts` with:

```ts
import { Kafka, type Producer } from "kafkajs";
import { config } from "../config/index.ts";
import type { NormalizedEvent } from "../types.ts";
import { buildKafkaConfig } from "./clientConfig.ts";

export type EventProducer = {
  publishRaw(resourceId: string, raw: unknown): Promise<void>;
  publishNormalized(event: NormalizedEvent): Promise<void>;
  publishDlq(reason: string, payload: unknown, key?: string): Promise<void>;
  isConnected(): boolean;
  disconnect(): Promise<void>;
};

export async function createProducer(clientId: string): Promise<EventProducer> {
  const kafka = new Kafka(buildKafkaConfig(clientId));
  const producer: Producer = kafka.producer({ allowAutoTopicCreation: true });
  await producer.connect();
  let connected = true;

  producer.on(producer.events.DISCONNECT, () => {
    connected = false;
  });
  producer.on(producer.events.CONNECT, () => {
    connected = true;
  });

  return {
    isConnected: () => connected,
    async disconnect() {
      await producer.disconnect();
    },
    async publishRaw(resourceId, raw) {
      await producer.send({
        topic: config.kafka.topics.raw,
        messages: [
          {
            key: resourceId,
            value: JSON.stringify({
              receivedAt: new Date().toISOString(),
              raw,
            }),
          },
        ],
      });
    },
    async publishNormalized(event) {
      await producer.send({
        topic: config.kafka.topics.events,
        messages: [
          {
            key: event.routingKey,
            value: JSON.stringify(event),
            headers: {
              source: event.source,
              eventType: event.eventType,
              severity: event.alert.severity,
              schemaVersion: String(event.schemaVersion),
              idempotencyKey: event.idempotencyKey,
            },
          },
        ],
      });
    },
    async publishDlq(reason, payload, key) {
      await producer.send({
        topic: config.kafka.topics.dlq,
        messages: [
          {
            key: key ?? null,
            value: JSON.stringify({
              receivedAt: new Date().toISOString(),
              reason,
              payload,
            }),
          },
        ],
      });
    },
  };
}
```

- [ ] **Step 4: Update the consumer to use the shared config**

Replace `src/kafka/consumer.ts` with:

```ts
import { Kafka, type Consumer } from "kafkajs";
import { buildKafkaConfig } from "./clientConfig.ts";

export async function createConsumer(clientId: string, groupId: string): Promise<Consumer> {
  const kafka = new Kafka(buildKafkaConfig(clientId));
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}
```

- [ ] **Step 5: Run typecheck and tests**

Run: `bun run typecheck && bun test`
Expected: All existing tests still pass. Typecheck clean.

- [ ] **Step 6: Commit**

```bash
git add src/kafka/clientConfig.ts src/kafka/producer.ts src/kafka/consumer.ts package.json bun.lock
git commit -m "SIO-789: add IAM SASL OAUTHBEARER for MSK Serverless"
```

---

## Task 3: Add Couchbase-disabled mode to writer

**Files:**
- Modify: `src/writer/index.ts`

- [ ] **Step 1: Update the writer to skip Couchbase when disabled**

Replace `src/writer/index.ts` with:

```ts
// src/writer/index.ts
import couchbase from "couchbase";
import { config } from "../config/index.ts";
import { getLogger } from "../logging/index.ts";
import { connectCouchbase } from "../couchbase/client.ts";
import {
  evolveState,
  historyDocKey,
  stateDocKey,
  toHistoryDoc,
} from "../couchbase/projection.ts";
import { createConsumer } from "../kafka/consumer.ts";
import { createProducer } from "../kafka/producer.ts";
import type { AutoOpsStateDoc, NormalizedEvent } from "../types.ts";

const log = getLogger("writer");

const cb = config.couchbase.enabled ? await connectCouchbase() : null;
if (!cb) {
  log.warn(
    { couchbaseEnabled: false },
    "couchbase disabled; events will be logged but not persisted",
  );
}

const dlqProducer = await createProducer(`${config.kafka.clientIdWriter}-dlq`);
const consumer = await createConsumer(config.kafka.clientIdWriter, config.kafka.groupId);
await consumer.subscribe({ topic: config.kafka.topics.events, fromBeginning: false });

log.info(
  { topic: config.kafka.topics.events, groupId: config.kafka.groupId },
  "writer consuming",
);

await consumer.run({
  eachMessage: async ({ message }) => {
    if (!message.value) return;
    const raw = message.value.toString();

    let event: NormalizedEvent;
    try {
      event = JSON.parse(raw) as NormalizedEvent;
      if (!event?.resource?.id || !event?.idempotencyKey) {
        throw new Error("missing required normalized fields");
      }
    } catch (err) {
      const reason = err instanceof Error ? err.message : "parse error";
      log.warn({ err, reason }, "invalid message, sending to DLQ");
      await dlqProducer.publishDlq(reason, raw, message.key?.toString() ?? undefined);
      return;
    }

    if (!cb) {
      log.info(
        {
          resourceId: event.resource.id,
          alertSignature: event.alertSignature,
          idempotencyKey: event.idempotencyKey,
          status: event.alert.status,
          severity: event.alert.severity,
        },
        "normalized event received (couchbase disabled)",
      );
      return;
    }

    const historyKey = historyDocKey(event);
    await cb.history.upsert(historyKey, toHistoryDoc(event));

    const stateKey = stateDocKey(event);
    let previous: AutoOpsStateDoc | null = null;
    try {
      const existing = await cb.state.get(stateKey);
      previous = existing.content as AutoOpsStateDoc;
    } catch (err) {
      if (err instanceof couchbase.DocumentNotFoundError) {
        previous = null;
      } else {
        throw err;
      }
    }
    await cb.state.upsert(stateKey, evolveState(previous, event));
  },
});

async function shutdown(signal: string) {
  log.info({ signal }, "shutting down writer");
  try {
    await consumer.disconnect();
    await dlqProducer.disconnect();
    if (cb) await cb.close();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
```

- [ ] **Step 2: Run typecheck and tests**

Run: `bun run typecheck && bun test`
Expected: All tests pass, typecheck clean.

- [ ] **Step 3: Smoke test locally with Couchbase disabled**

Run (in one shell):

```bash
docker compose up -d redpanda
COUCHBASE_ENABLED=false bun run src/writer/index.ts &
sleep 3
COUCHBASE_ENABLED=false bun run src/gateway/index.ts &
sleep 3
curl -s -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d '{"resourceId":"r-1","resourceName":"x","title":"t","severity":"High","status":"open","startTime":"2026-05-18T19:27:40Z"}'
```

Expected: 202 response from gateway. Writer logs an INFO line with `"normalized event received (couchbase disabled)"`.

Cleanup:

```bash
kill %1 %2
docker compose down
```

- [ ] **Step 4: Commit**

```bash
git add src/writer/index.ts
git commit -m "SIO-789: skip Couchbase when COUCHBASE_ENABLED=false"
```

---

## Task 4: Add Dockerfile

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Write the Dockerfile**

Create `Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1.7
FROM oven/bun:1-alpine AS deps
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM oven/bun:1-alpine
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json bun.lock tsconfig.json ./
COPY src ./src
USER bun
# ECS task definitions set the command per service.
```

- [ ] **Step 2: Write the .dockerignore**

Create `.dockerignore`:

```
.git
.github
.claude
.env
.env.*
!.env.example
node_modules
test
docs
scripts
docker-compose.yml
README.md
*.md
bunfig.toml
```

Note: `bunfig.toml` is excluded because it scopes `test.root = "./test"` which we don't need in the image.

- [ ] **Step 3: Build the image locally and smoke test**

Run:

```bash
docker build -t eventgate:local .
docker run --rm -e COUCHBASE_ENABLED=false -e KAFKA_BROKERS=stub:9092 \
  eventgate:local bun --version
```

Expected: Image builds successfully (~30-90 seconds). The `bun --version` call prints a version like `1.x.x`.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "SIO-789: add multi-stage Bun Dockerfile"
```

---

## Task 5: Update .env.example and .gitignore

**Files:**
- Modify: `.env.example`
- Modify: `.gitignore`

- [ ] **Step 1: Read the current .env.example**

Run: `cat .env.example`
Note the current contents — the next step appends to them.

- [ ] **Step 2: Append the AWS env vars to .env.example**

Append to `.env.example`:

```bash

# --- AWS deployment ---
# Kafka SASL mechanism: "none" for local Redpanda, "iam" for AWS MSK Serverless.
KAFKA_AUTH=none
# AWS region for MSK IAM SASL token signing. Required when KAFKA_AUTH=iam.
KAFKA_REGION=
# Set to "false" to skip Couchbase entirely (writer logs events only). Defaults to "true".
COUCHBASE_ENABLED=true
```

- [ ] **Step 3: Update .gitignore**

Append to `.gitignore`:

```
# AWS deployment state (resource ARNs, IDs)
scripts/deploy/.env.aws
```

- [ ] **Step 4: Commit**

```bash
git add .env.example .gitignore
git commit -m "SIO-789: document AWS deployment env vars"
```

---

## Task 6: Create deploy script shared helpers

**Files:**
- Create: `scripts/deploy/lib.sh`
- Create: `scripts/deploy/README.md`

- [ ] **Step 1: Write the shared helpers**

Create `scripts/deploy/lib.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/lib.sh
# Shared helpers for the eventgate AWS deploy scripts.
# Source this from every deploy script: `source "$(dirname "$0")/lib.sh"`.

set -euo pipefail

export AWS_REGION="${AWS_REGION:-eu-central-1}"
export AWS_PAGER=""

readonly ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.env.aws"

log() { printf '%s [%s] %s\n' "$(date -u +%H:%M:%S)" "${SCRIPT_NAME:-deploy}" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# Read a key from .env.aws (returns empty string if missing).
read_env() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    echo ""
    return 0
  fi
  grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true
}

# Write or update a key in .env.aws.
write_env() {
  local key="$1"
  local value="$2"
  mkdir -p "$(dirname "$ENV_FILE")"
  touch "$ENV_FILE"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    # Use a temp file to avoid sed -i portability issues between GNU and BSD.
    awk -v k="$key" -v v="$value" -F= '
      $1 == k { print k "=" v; next }
      { print }
    ' "$ENV_FILE" > "$ENV_FILE.tmp"
    mv "$ENV_FILE.tmp" "$ENV_FILE"
  else
    echo "${key}=${value}" >> "$ENV_FILE"
  fi
  log "wrote $key=$value to .env.aws"
}

require_env() {
  local key="$1"
  local value
  value="$(read_env "$key")"
  if [[ -z "$value" ]]; then
    die "$key is not set in $ENV_FILE — run the earlier phase first"
  fi
  echo "$value"
}

aws_account_id() {
  aws sts get-caller-identity --query Account --output text
}
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x scripts/deploy/lib.sh`

- [ ] **Step 3: Write the deploy README**

Create `scripts/deploy/README.md`:

```markdown
# AWS deploy scripts (manual, via AWS MCP)

Scripts to stand up eventgate on AWS ECS Fargate in `eu-central-1`.

## Prerequisites
- `aws` CLI v2, authenticated to the target account (env vars or `~/.aws/credentials`).
- `docker` for building images.
- Bun for the topic-creation script.

## Phases

| Phase | Script | What it does |
|---|---|---|
| A.1 | `01-network.sh` | VPC, subnets, IGW, NAT |
| A.2 | `02-security-groups.sh` | All SGs |
| A.3 | `03-msk.sh` | MSK Serverless cluster (~10 min) |
| A.4 | `04-ecr.sh` | ECR repo |
| A.5 | `05-ecs-cluster.sh` | ECS cluster |
| A.6 | `06-log-groups.sh` | CloudWatch log groups |
| A.7 | `07-alb.sh` | ALB + target group + listener |
| A.8 | `08-iam-roles.sh` | Task execution + task roles |
| B | `build-and-push.sh` | Build image + push to ECR |
| C | `09-create-topics.ts` | Pre-create Kafka topics |
| D.1 | `10-register-task-defs.sh` | Register task definitions |
| D.2 | `11-deploy-services.sh` | Create/update ECS services |
| D.3 | `12-print-url.sh` | Print the ALB DNS |
| Cleanup | `teardown.sh` | Delete everything (in reverse order) |

All scripts share state via `scripts/deploy/.env.aws` (gitignored). They are idempotent — safe to re-run.

Run in order on first deploy:

```bash
cd scripts/deploy
./01-network.sh
./02-security-groups.sh
./03-msk.sh        # blocks until cluster is ACTIVE
./04-ecr.sh
./05-ecs-cluster.sh
./06-log-groups.sh
./07-alb.sh
./08-iam-roles.sh
./build-and-push.sh
bun 09-create-topics.ts
./10-register-task-defs.sh
./11-deploy-services.sh
./12-print-url.sh
```
```

- [ ] **Step 4: Commit**

```bash
git add scripts/deploy/lib.sh scripts/deploy/README.md
git commit -m "SIO-789: add deploy script shared helpers and README"
```

---

## Task 7: Phase A.1 — Network (VPC, subnets, NAT)

**Files:**
- Create: `scripts/deploy/01-network.sh`

- [ ] **Step 1: Write the script**

Create `scripts/deploy/01-network.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/01-network.sh
# Creates VPC, 2 public + 2 private subnets, IGW, NAT GW.

SCRIPT_NAME="01-network"
source "$(dirname "$0")/lib.sh"

readonly VPC_CIDR="10.0.0.0/16"
readonly AZ_A="${AWS_REGION}a"
readonly AZ_B="${AWS_REGION}b"

existing="$(aws ec2 describe-vpcs --filters Name=tag:Name,Values=eventgate-vpc \
  --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)"

if [[ "$existing" != "None" && -n "$existing" ]]; then
  log "VPC already exists: $existing"
  vpc_id="$existing"
else
  vpc_id="$(aws ec2 create-vpc --cidr-block "$VPC_CIDR" \
    --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=eventgate-vpc}]" \
    --query 'Vpc.VpcId' --output text)"
  aws ec2 modify-vpc-attribute --vpc-id "$vpc_id" --enable-dns-hostnames
  aws ec2 modify-vpc-attribute --vpc-id "$vpc_id" --enable-dns-support
  log "created VPC $vpc_id"
fi
write_env VPC_ID "$vpc_id"

create_subnet() {
  local name="$1" cidr="$2" az="$3"
  local sid
  sid="$(aws ec2 describe-subnets --filters Name=tag:Name,Values="$name" \
    --query 'Subnets[0].SubnetId' --output text 2>/dev/null || echo None)"
  if [[ "$sid" == "None" || -z "$sid" ]]; then
    sid="$(aws ec2 create-subnet --vpc-id "$vpc_id" --cidr-block "$cidr" \
      --availability-zone "$az" \
      --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=$name}]" \
      --query 'Subnet.SubnetId' --output text)"
    log "created subnet $name=$sid"
  else
    log "subnet $name already exists: $sid"
  fi
  echo "$sid"
}

pub_a="$(create_subnet eventgate-public-a 10.0.0.0/24 "$AZ_A")"
pub_b="$(create_subnet eventgate-public-b 10.0.1.0/24 "$AZ_B")"
prv_a="$(create_subnet eventgate-private-a 10.0.10.0/24 "$AZ_A")"
prv_b="$(create_subnet eventgate-private-b 10.0.11.0/24 "$AZ_B")"

write_env PUBLIC_SUBNET_A "$pub_a"
write_env PUBLIC_SUBNET_B "$pub_b"
write_env PRIVATE_SUBNET_A "$prv_a"
write_env PRIVATE_SUBNET_B "$prv_b"

# Public subnets get auto-assign public IP for ALB nodes.
aws ec2 modify-subnet-attribute --subnet-id "$pub_a" --map-public-ip-on-launch
aws ec2 modify-subnet-attribute --subnet-id "$pub_b" --map-public-ip-on-launch

igw_id="$(aws ec2 describe-internet-gateways \
  --filters Name=attachment.vpc-id,Values="$vpc_id" \
  --query 'InternetGateways[0].InternetGatewayId' --output text 2>/dev/null || echo None)"
if [[ "$igw_id" == "None" || -z "$igw_id" ]]; then
  igw_id="$(aws ec2 create-internet-gateway \
    --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=eventgate-igw}]" \
    --query 'InternetGateway.InternetGatewayId' --output text)"
  aws ec2 attach-internet-gateway --vpc-id "$vpc_id" --internet-gateway-id "$igw_id"
  log "created and attached IGW $igw_id"
fi
write_env IGW_ID "$igw_id"

eip_id="$(read_env NAT_EIP_ID)"
if [[ -z "$eip_id" ]]; then
  eip_id="$(aws ec2 allocate-address --domain vpc \
    --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=eventgate-nat-eip}]" \
    --query AllocationId --output text)"
  log "allocated NAT EIP $eip_id"
fi
write_env NAT_EIP_ID "$eip_id"

nat_id="$(aws ec2 describe-nat-gateways \
  --filter Name=tag:Name,Values=eventgate-nat Name=state,Values=available,pending \
  --query 'NatGateways[0].NatGatewayId' --output text 2>/dev/null || echo None)"
if [[ "$nat_id" == "None" || -z "$nat_id" ]]; then
  nat_id="$(aws ec2 create-nat-gateway --subnet-id "$pub_a" --allocation-id "$eip_id" \
    --tag-specifications "ResourceType=natgateway,Tags=[{Key=Name,Value=eventgate-nat}]" \
    --query 'NatGateway.NatGatewayId' --output text)"
  log "created NAT GW $nat_id; waiting for it to be available"
  aws ec2 wait nat-gateway-available --nat-gateway-ids "$nat_id"
fi
write_env NAT_GW_ID "$nat_id"

# Route tables. One public (default route to IGW), one private (default route to NAT).
ensure_rt() {
  local name="$1"
  local rid
  rid="$(aws ec2 describe-route-tables --filters Name=tag:Name,Values="$name" \
    --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null || echo None)"
  if [[ "$rid" == "None" || -z "$rid" ]]; then
    rid="$(aws ec2 create-route-table --vpc-id "$vpc_id" \
      --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=$name}]" \
      --query 'RouteTable.RouteTableId' --output text)"
  fi
  echo "$rid"
}

pub_rt="$(ensure_rt eventgate-public-rt)"
prv_rt="$(ensure_rt eventgate-private-rt)"
write_env PUBLIC_RT_ID "$pub_rt"
write_env PRIVATE_RT_ID "$prv_rt"

aws ec2 create-route --route-table-id "$pub_rt" --destination-cidr-block 0.0.0.0/0 \
  --gateway-id "$igw_id" 2>/dev/null || log "public default route exists"
aws ec2 create-route --route-table-id "$prv_rt" --destination-cidr-block 0.0.0.0/0 \
  --nat-gateway-id "$nat_id" 2>/dev/null || log "private default route exists"

associate_rt() {
  local rt="$1" subnet="$2"
  local existing
  existing="$(aws ec2 describe-route-tables --route-table-ids "$rt" \
    --query "RouteTables[0].Associations[?SubnetId=='$subnet'].RouteTableAssociationId" \
    --output text)"
  if [[ -z "$existing" ]]; then
    aws ec2 associate-route-table --route-table-id "$rt" --subnet-id "$subnet" >/dev/null
    log "associated $subnet with $rt"
  fi
}

associate_rt "$pub_rt" "$pub_a"
associate_rt "$pub_rt" "$pub_b"
associate_rt "$prv_rt" "$prv_a"
associate_rt "$prv_rt" "$prv_b"

log "network done"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x scripts/deploy/01-network.sh`

- [ ] **Step 3: Commit**

```bash
git add scripts/deploy/01-network.sh
git commit -m "SIO-789: phase A.1 deploy script for VPC + subnets + NAT"
```

---

## Task 8: Phase A.2 — Security groups

**Files:**
- Create: `scripts/deploy/02-security-groups.sh`

- [ ] **Step 1: Write the script**

Create `scripts/deploy/02-security-groups.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/02-security-groups.sh
# Creates sg-alb, sg-gateway, sg-writer, sg-msk and wires rules.

SCRIPT_NAME="02-security-groups"
source "$(dirname "$0")/lib.sh"

vpc_id="$(require_env VPC_ID)"

ensure_sg() {
  local name="$1" desc="$2"
  local sgid
  sgid="$(aws ec2 describe-security-groups \
    --filters Name=vpc-id,Values="$vpc_id" Name=group-name,Values="$name" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo None)"
  if [[ "$sgid" == "None" || -z "$sgid" ]]; then
    sgid="$(aws ec2 create-security-group --vpc-id "$vpc_id" --group-name "$name" \
      --description "$desc" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$name}]" \
      --query GroupId --output text)"
    log "created SG $name=$sgid"
  else
    log "SG $name already exists: $sgid"
  fi
  echo "$sgid"
}

sg_alb="$(ensure_sg eventgate-alb 'eventgate ALB ingress :80')"
sg_gw="$(ensure_sg eventgate-gateway 'eventgate gateway tasks')"
sg_wr="$(ensure_sg eventgate-writer 'eventgate writer tasks')"
sg_msk="$(ensure_sg eventgate-msk 'eventgate MSK Serverless')"

write_env SG_ALB "$sg_alb"
write_env SG_GATEWAY "$sg_gw"
write_env SG_WRITER "$sg_wr"
write_env SG_MSK "$sg_msk"

ingress_cidr() {
  local sg="$1" port="$2" cidr="$3"
  aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --protocol tcp --port "$port" --cidr "$cidr" 2>/dev/null \
    || log "ingress $cidr:$port on $sg already exists"
}

ingress_sg() {
  local sg="$1" port="$2" source_sg="$3"
  aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --ip-permissions "IpProtocol=tcp,FromPort=$port,ToPort=$port,UserIdGroupPairs=[{GroupId=$source_sg}]" \
    2>/dev/null || log "ingress sg=$source_sg:$port on $sg already exists"
}

ingress_cidr "$sg_alb" 80 0.0.0.0/0
ingress_sg   "$sg_gw"  3000 "$sg_alb"
ingress_sg   "$sg_msk" 9098 "$sg_gw"
ingress_sg   "$sg_msk" 9098 "$sg_wr"

log "security groups done"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/02-security-groups.sh
git add scripts/deploy/02-security-groups.sh
git commit -m "SIO-789: phase A.2 deploy script for security groups"
```

---

## Task 9: Phase A.3 — MSK Serverless cluster

**Files:**
- Create: `scripts/deploy/03-msk.sh`

- [ ] **Step 1: Write the script**

Create `scripts/deploy/03-msk.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/03-msk.sh
# Creates an MSK Serverless cluster with IAM auth.

SCRIPT_NAME="03-msk"
source "$(dirname "$0")/lib.sh"

prv_a="$(require_env PRIVATE_SUBNET_A)"
prv_b="$(require_env PRIVATE_SUBNET_B)"
sg_msk="$(require_env SG_MSK)"

existing_arn="$(aws kafka list-clusters-v2 \
  --cluster-name-filter eventgate-msk \
  --query 'ClusterInfoList[0].ClusterArn' --output text 2>/dev/null || echo None)"

if [[ "$existing_arn" != "None" && -n "$existing_arn" ]]; then
  log "MSK cluster already exists: $existing_arn"
  cluster_arn="$existing_arn"
else
  cluster_arn="$(aws kafka create-cluster-v2 \
    --cluster-name eventgate-msk \
    --serverless "VpcConfigs=[{SubnetIds=[$prv_a,$prv_b],SecurityGroupIds=[$sg_msk]}],ClientAuthentication={Sasl={Iam={Enabled=true}}}" \
    --query ClusterArn --output text)"
  log "created MSK Serverless cluster: $cluster_arn"
fi

write_env MSK_CLUSTER_ARN "$cluster_arn"

log "waiting for MSK cluster to become ACTIVE (this can take ~10 min)..."
while true; do
  state="$(aws kafka describe-cluster-v2 --cluster-arn "$cluster_arn" \
    --query 'ClusterInfo.State' --output text)"
  log "MSK state: $state"
  case "$state" in
    ACTIVE) break ;;
    FAILED|DELETING|MAINTENANCE) die "MSK cluster reached unexpected state: $state" ;;
    *) sleep 30 ;;
  esac
done

bootstrap="$(aws kafka get-bootstrap-brokers --cluster-arn "$cluster_arn" \
  --query 'BootstrapBrokerStringSaslIam' --output text)"
write_env MSK_BOOTSTRAP "$bootstrap"
log "MSK bootstrap brokers: $bootstrap"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/03-msk.sh
git add scripts/deploy/03-msk.sh
git commit -m "SIO-789: phase A.3 deploy script for MSK Serverless cluster"
```

---

## Task 10: Phase A.4-A.6 — ECR, ECS cluster, log groups

**Files:**
- Create: `scripts/deploy/04-ecr.sh`
- Create: `scripts/deploy/05-ecs-cluster.sh`
- Create: `scripts/deploy/06-log-groups.sh`

- [ ] **Step 1: Write the ECR script**

Create `scripts/deploy/04-ecr.sh`:

```bash
#!/usr/bin/env bash
SCRIPT_NAME="04-ecr"
source "$(dirname "$0")/lib.sh"

existing="$(aws ecr describe-repositories --repository-names eventgate \
  --query 'repositories[0].repositoryUri' --output text 2>/dev/null || echo None)"

if [[ "$existing" != "None" && -n "$existing" ]]; then
  log "ECR repo already exists: $existing"
  uri="$existing"
else
  uri="$(aws ecr create-repository --repository-name eventgate \
    --image-scanning-configuration scanOnPush=true \
    --query 'repository.repositoryUri' --output text)"
  log "created ECR repo: $uri"
fi
write_env ECR_REPO_URI "$uri"
```

- [ ] **Step 2: Write the ECS cluster script**

Create `scripts/deploy/05-ecs-cluster.sh`:

```bash
#!/usr/bin/env bash
SCRIPT_NAME="05-ecs-cluster"
source "$(dirname "$0")/lib.sh"

existing="$(aws ecs describe-clusters --clusters eventgate \
  --query 'clusters[?status==`ACTIVE`].clusterArn | [0]' --output text 2>/dev/null || echo None)"

if [[ "$existing" != "None" && -n "$existing" ]]; then
  log "ECS cluster already exists: $existing"
  arn="$existing"
else
  arn="$(aws ecs create-cluster --cluster-name eventgate \
    --capacity-providers FARGATE --query 'cluster.clusterArn' --output text)"
  log "created ECS cluster: $arn"
fi
write_env ECS_CLUSTER_ARN "$arn"
```

- [ ] **Step 3: Write the log groups script**

Create `scripts/deploy/06-log-groups.sh`:

```bash
#!/usr/bin/env bash
SCRIPT_NAME="06-log-groups"
source "$(dirname "$0")/lib.sh"

ensure_log_group() {
  local name="$1"
  if aws logs describe-log-groups --log-group-name-prefix "$name" \
    --query "logGroups[?logGroupName=='$name'] | length(@)" --output text | grep -q '^0$'; then
    aws logs create-log-group --log-group-name "$name"
    aws logs put-retention-policy --log-group-name "$name" --retention-in-days 7
    log "created log group $name"
  else
    log "log group $name already exists"
  fi
}

ensure_log_group /eventgate/gateway
ensure_log_group /eventgate/writer
```

- [ ] **Step 4: Make all three executable and commit**

```bash
chmod +x scripts/deploy/04-ecr.sh scripts/deploy/05-ecs-cluster.sh scripts/deploy/06-log-groups.sh
git add scripts/deploy/04-ecr.sh scripts/deploy/05-ecs-cluster.sh scripts/deploy/06-log-groups.sh
git commit -m "SIO-789: phase A.4-A.6 deploy scripts for ECR, ECS cluster, log groups"
```

---

## Task 11: Phase A.7 — ALB + target group + listener

**Files:**
- Create: `scripts/deploy/07-alb.sh`

- [ ] **Step 1: Write the ALB script**

Create `scripts/deploy/07-alb.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/07-alb.sh
# Creates the public ALB, target group, and HTTP :80 listener.

SCRIPT_NAME="07-alb"
source "$(dirname "$0")/lib.sh"

vpc_id="$(require_env VPC_ID)"
pub_a="$(require_env PUBLIC_SUBNET_A)"
pub_b="$(require_env PUBLIC_SUBNET_B)"
sg_alb="$(require_env SG_ALB)"

# Target group first (listener forwards to it).
tg_arn="$(aws elbv2 describe-target-groups --names eventgate-gateway-tg \
  --query 'TargetGroups[0].TargetGroupArn' --output text 2>/dev/null || echo None)"
if [[ "$tg_arn" == "None" || -z "$tg_arn" ]]; then
  tg_arn="$(aws elbv2 create-target-group \
    --name eventgate-gateway-tg \
    --protocol HTTP --port 3000 \
    --vpc-id "$vpc_id" \
    --target-type ip \
    --health-check-protocol HTTP \
    --health-check-path /healthz \
    --health-check-interval-seconds 30 \
    --healthy-threshold-count 2 \
    --unhealthy-threshold-count 3 \
    --query 'TargetGroups[0].TargetGroupArn' --output text)"
  log "created target group: $tg_arn"
else
  log "target group already exists: $tg_arn"
fi
write_env GATEWAY_TG_ARN "$tg_arn"

# ALB.
alb_arn="$(aws elbv2 describe-load-balancers --names eventgate-alb \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text 2>/dev/null || echo None)"
if [[ "$alb_arn" == "None" || -z "$alb_arn" ]]; then
  alb_arn="$(aws elbv2 create-load-balancer \
    --name eventgate-alb \
    --type application \
    --scheme internet-facing \
    --subnets "$pub_a" "$pub_b" \
    --security-groups "$sg_alb" \
    --query 'LoadBalancers[0].LoadBalancerArn' --output text)"
  log "created ALB: $alb_arn"
else
  log "ALB already exists: $alb_arn"
fi
write_env ALB_ARN "$alb_arn"

alb_dns="$(aws elbv2 describe-load-balancers --load-balancer-arns "$alb_arn" \
  --query 'LoadBalancers[0].DNSName' --output text)"
write_env ALB_DNS "$alb_dns"

# Listener.
listener_arn="$(aws elbv2 describe-listeners --load-balancer-arn "$alb_arn" \
  --query "Listeners[?Port==\`80\`].ListenerArn | [0]" --output text 2>/dev/null || echo None)"
if [[ "$listener_arn" == "None" || -z "$listener_arn" ]]; then
  listener_arn="$(aws elbv2 create-listener \
    --load-balancer-arn "$alb_arn" \
    --protocol HTTP --port 80 \
    --default-actions "Type=forward,TargetGroupArn=$tg_arn" \
    --query 'Listeners[0].ListenerArn' --output text)"
  log "created listener: $listener_arn"
else
  log "listener already exists: $listener_arn"
fi
write_env LISTENER_ARN "$listener_arn"

log "ALB DNS: $alb_dns"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/07-alb.sh
git add scripts/deploy/07-alb.sh
git commit -m "SIO-789: phase A.7 deploy script for ALB"
```

---

## Task 12: Phase A.8 — IAM roles

**Files:**
- Create: `scripts/deploy/08-iam-roles.sh`

- [ ] **Step 1: Write the IAM roles script**

Create `scripts/deploy/08-iam-roles.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/08-iam-roles.sh
# Creates the task execution role and per-service task roles with MSK perms.

SCRIPT_NAME="08-iam-roles"
source "$(dirname "$0")/lib.sh"

cluster_arn="$(require_env MSK_CLUSTER_ARN)"
account="$(aws_account_id)"

readonly TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

ensure_role() {
  local name="$1"
  if aws iam get-role --role-name "$name" >/dev/null 2>&1; then
    log "role $name already exists"
  else
    aws iam create-role --role-name "$name" \
      --assume-role-policy-document "$TRUST" >/dev/null
    log "created role $name"
  fi
}

ensure_role eventgate-task-execution
ensure_role eventgate-gateway-task
ensure_role eventgate-writer-task

# Task execution role: pull image from ECR + write logs.
aws iam attach-role-policy --role-name eventgate-task-execution \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

# Build MSK perms scoped to this cluster. Topic ARNs use a wildcard.
# Cluster ARN format: arn:aws:kafka:region:account:cluster/name/uuid
cluster_id="$(echo "$cluster_arn" | awk -F'/' '{print $NF}')"
cluster_name="$(echo "$cluster_arn" | awk -F'/' '{print $2}')"
readonly TOPIC_ARN_PREFIX="arn:aws:kafka:${AWS_REGION}:${account}:topic/${cluster_name}/${cluster_id}"
readonly GROUP_ARN_PREFIX="arn:aws:kafka:${AWS_REGION}:${account}:group/${cluster_name}/${cluster_id}"

msk_policy_doc() {
  cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:AlterCluster"
      ],
      "Resource": "$cluster_arn"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeTopicDynamicConfiguration",
        "kafka-cluster:WriteDataIdempotently"
      ],
      "Resource": "${TOPIC_ARN_PREFIX}/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": "${GROUP_ARN_PREFIX}/*"
    }
  ]
}
EOF
}

put_inline() {
  local role="$1"
  aws iam put-role-policy --role-name "$role" \
    --policy-name eventgate-msk-access \
    --policy-document "$(msk_policy_doc)"
  log "wrote MSK inline policy on $role"
}

put_inline eventgate-gateway-task
put_inline eventgate-writer-task

exec_arn="$(aws iam get-role --role-name eventgate-task-execution --query 'Role.Arn' --output text)"
gw_arn="$(aws iam get-role --role-name eventgate-gateway-task --query 'Role.Arn' --output text)"
wr_arn="$(aws iam get-role --role-name eventgate-writer-task --query 'Role.Arn' --output text)"

write_env TASK_EXECUTION_ROLE_ARN "$exec_arn"
write_env GATEWAY_TASK_ROLE_ARN "$gw_arn"
write_env WRITER_TASK_ROLE_ARN "$wr_arn"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/08-iam-roles.sh
git add scripts/deploy/08-iam-roles.sh
git commit -m "SIO-789: phase A.8 deploy script for IAM roles"
```

---

## Task 13: Phase B — Build and push the image

**Files:**
- Create: `scripts/deploy/build-and-push.sh`

- [ ] **Step 1: Write the build-and-push script**

Create `scripts/deploy/build-and-push.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/build-and-push.sh
# Build the eventgate image and push to ECR. Tags with git short SHA + 'latest'.

SCRIPT_NAME="build-and-push"
source "$(dirname "$0")/lib.sh"

repo_uri="$(require_env ECR_REPO_URI)"
account="$(aws_account_id)"
sha="$(git rev-parse --short HEAD)"

# repo_uri is e.g. 123456789012.dkr.ecr.eu-central-1.amazonaws.com/eventgate
registry="${repo_uri%/eventgate}"

log "logging into ECR ($registry)"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$registry"

log "building eventgate:$sha"
docker build --platform linux/amd64 -t "eventgate:$sha" \
  "$(git rev-parse --show-toplevel)"

docker tag "eventgate:$sha" "$repo_uri:$sha"
docker tag "eventgate:$sha" "$repo_uri:latest"

log "pushing $repo_uri:$sha and :latest"
docker push "$repo_uri:$sha"
docker push "$repo_uri:latest"

write_env IMAGE_TAG "$sha"
write_env IMAGE_URI "$repo_uri:$sha"
log "image pushed: $repo_uri:$sha"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/build-and-push.sh
git add scripts/deploy/build-and-push.sh
git commit -m "SIO-789: phase B build-and-push script"
```

---

## Task 14: Phase C — Create Kafka topics

**Files:**
- Create: `scripts/deploy/09-create-topics.ts`

- [ ] **Step 1: Write the topic-creation script**

Create `scripts/deploy/09-create-topics.ts`:

```ts
// scripts/deploy/09-create-topics.ts
// Pre-creates the three eventgate topics on MSK Serverless via kafkajs admin.
// Run: bun scripts/deploy/09-create-topics.ts
import { generateAuthToken } from "aws-msk-iam-sasl-signer-js";
import { Kafka, logLevel } from "kafkajs";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const envFile = resolve(import.meta.dir, ".env.aws");
const env = Object.fromEntries(
  readFileSync(envFile, "utf8")
    .split("\n")
    .filter((l) => l.includes("="))
    .map((l) => {
      const idx = l.indexOf("=");
      return [l.slice(0, idx), l.slice(idx + 1)];
    }),
) as Record<string, string>;

const bootstrap = env.MSK_BOOTSTRAP;
const region = process.env.AWS_REGION ?? "eu-central-1";
if (!bootstrap) throw new Error("MSK_BOOTSTRAP missing in .env.aws — run 03-msk.sh first");

const kafka = new Kafka({
  clientId: "eventgate-topic-bootstrap",
  brokers: bootstrap.split(","),
  ssl: true,
  sasl: {
    mechanism: "oauthbearer",
    oauthBearerProvider: async () => {
      const { token } = await generateAuthToken({ region });
      return { value: token };
    },
  },
  logLevel: logLevel.INFO,
});

const admin = kafka.admin();
await admin.connect();

const desired = [
  { topic: "ops.elastic.autoops.raw.v1", numPartitions: 3, replicationFactor: 3 },
  { topic: "ops.elastic.autoops.events.v1", numPartitions: 3, replicationFactor: 3 },
  { topic: "ops.elastic.autoops.dlq.v1", numPartitions: 3, replicationFactor: 3 },
];

const existing = new Set(await admin.listTopics());
const toCreate = desired.filter((t) => !existing.has(t.topic));

if (toCreate.length === 0) {
  console.log("all topics already exist");
} else {
  await admin.createTopics({ topics: toCreate });
  console.log(`created topics: ${toCreate.map((t) => t.topic).join(", ")}`);
}

await admin.disconnect();
```

Note on `replicationFactor: 3`: MSK Serverless requires RF=3 (managed). Setting it explicitly here is documentation; the broker enforces it regardless.

- [ ] **Step 2: Test locally that it parses and typechecks**

Run: `bun run typecheck`
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add scripts/deploy/09-create-topics.ts
git commit -m "SIO-789: phase C topic-bootstrap Bun script"
```

---

## Task 15: Phase D.1 — Register task definitions

**Files:**
- Create: `scripts/deploy/10-register-task-defs.sh`

- [ ] **Step 1: Write the task-definition script**

Create `scripts/deploy/10-register-task-defs.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/10-register-task-defs.sh
# Registers task definitions for the gateway and writer services.

SCRIPT_NAME="10-register-task-defs"
source "$(dirname "$0")/lib.sh"

image_uri="$(require_env IMAGE_URI)"
exec_role="$(require_env TASK_EXECUTION_ROLE_ARN)"
gw_role="$(require_env GATEWAY_TASK_ROLE_ARN)"
wr_role="$(require_env WRITER_TASK_ROLE_ARN)"
bootstrap="$(require_env MSK_BOOTSTRAP)"

# brokers as a comma-separated string for KAFKA_BROKERS env
brokers="$bootstrap"

env_block_common='[
  {"name":"ENVIRONMENT","value":"prod"},
  {"name":"KAFKA_AUTH","value":"iam"},
  {"name":"KAFKA_REGION","value":"'"$AWS_REGION"'"},
  {"name":"KAFKA_BROKERS","value":"'"$brokers"'"},
  {"name":"COUCHBASE_ENABLED","value":"false"},
  {"name":"LOG_LEVEL","value":"info"}
]'

register() {
  local family="$1" role_arn="$2" command_json="$3" log_group="$4"
  local container_def
  container_def="$(cat <<EOF
[
  {
    "name": "$family",
    "image": "$image_uri",
    "essential": true,
    "command": $command_json,
    "portMappings": $( [[ "$family" == "eventgate-gateway" ]] && echo '[{"containerPort":3000,"protocol":"tcp"}]' || echo '[]' ),
    "environment": $env_block_common,
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "$log_group",
        "awslogs-region": "$AWS_REGION",
        "awslogs-stream-prefix": "ecs"
      }
    }
  }
]
EOF
)"

  aws ecs register-task-definition \
    --family "$family" \
    --network-mode awsvpc \
    --requires-compatibilities FARGATE \
    --cpu 256 --memory 512 \
    --execution-role-arn "$exec_role" \
    --task-role-arn "$role_arn" \
    --runtime-platform "operatingSystemFamily=LINUX,cpuArchitecture=X86_64" \
    --container-definitions "$container_def" \
    --query 'taskDefinition.taskDefinitionArn' --output text
}

gw_arn="$(register eventgate-gateway "$gw_role" \
  '["bun","run","src/gateway/index.ts"]' /eventgate/gateway)"
log "registered gateway task def: $gw_arn"
write_env GATEWAY_TASK_DEF_ARN "$gw_arn"

wr_arn="$(register eventgate-writer "$wr_role" \
  '["bun","run","src/writer/index.ts"]' /eventgate/writer)"
log "registered writer task def: $wr_arn"
write_env WRITER_TASK_DEF_ARN "$wr_arn"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/10-register-task-defs.sh
git add scripts/deploy/10-register-task-defs.sh
git commit -m "SIO-789: phase D.1 task-definition registration script"
```

---

## Task 16: Phase D.2 — Create or update ECS services

**Files:**
- Create: `scripts/deploy/11-deploy-services.sh`

- [ ] **Step 1: Write the deploy-services script**

Create `scripts/deploy/11-deploy-services.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/11-deploy-services.sh
# Create or update the gateway and writer ECS services.

SCRIPT_NAME="11-deploy-services"
source "$(dirname "$0")/lib.sh"

cluster_arn="$(require_env ECS_CLUSTER_ARN)"
prv_a="$(require_env PRIVATE_SUBNET_A)"
prv_b="$(require_env PRIVATE_SUBNET_B)"
sg_gw="$(require_env SG_GATEWAY)"
sg_wr="$(require_env SG_WRITER)"
tg_arn="$(require_env GATEWAY_TG_ARN)"
gw_td="$(require_env GATEWAY_TASK_DEF_ARN)"
wr_td="$(require_env WRITER_TASK_DEF_ARN)"

service_exists() {
  local svc="$1"
  local status
  status="$(aws ecs describe-services --cluster "$cluster_arn" --services "$svc" \
    --query 'services[0].status' --output text 2>/dev/null || echo NONE)"
  [[ "$status" == "ACTIVE" ]]
}

# Gateway service (attached to ALB target group).
if service_exists eventgate-gateway; then
  log "updating gateway service to new task def"
  aws ecs update-service \
    --cluster "$cluster_arn" \
    --service eventgate-gateway \
    --task-definition "$gw_td" \
    --force-new-deployment >/dev/null
else
  log "creating gateway service"
  aws ecs create-service \
    --cluster "$cluster_arn" \
    --service-name eventgate-gateway \
    --task-definition "$gw_td" \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$prv_a,$prv_b],securityGroups=[$sg_gw],assignPublicIp=DISABLED}" \
    --load-balancers "targetGroupArn=$tg_arn,containerName=eventgate-gateway,containerPort=3000" \
    --health-check-grace-period-seconds 60 >/dev/null
fi

# Writer service (no LB).
if service_exists eventgate-writer; then
  log "updating writer service to new task def"
  aws ecs update-service \
    --cluster "$cluster_arn" \
    --service eventgate-writer \
    --task-definition "$wr_td" \
    --force-new-deployment >/dev/null
else
  log "creating writer service"
  aws ecs create-service \
    --cluster "$cluster_arn" \
    --service-name eventgate-writer \
    --task-definition "$wr_td" \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$prv_a,$prv_b],securityGroups=[$sg_wr],assignPublicIp=DISABLED}" >/dev/null
fi

log "waiting for gateway and writer to become stable"
aws ecs wait services-stable --cluster "$cluster_arn" \
  --services eventgate-gateway eventgate-writer
log "both services stable"
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/11-deploy-services.sh
git add scripts/deploy/11-deploy-services.sh
git commit -m "SIO-789: phase D.2 ECS service deploy script"
```

---

## Task 17: Phase D.3 — Print URL

**Files:**
- Create: `scripts/deploy/12-print-url.sh`

- [ ] **Step 1: Write the print-url script**

Create `scripts/deploy/12-print-url.sh`:

```bash
#!/usr/bin/env bash
SCRIPT_NAME="12-print-url"
source "$(dirname "$0")/lib.sh"

alb_dns="$(require_env ALB_DNS)"

echo
echo "=================================================="
echo "Webhook URL for Elastic AutoOps:"
echo "  http://${alb_dns}/webhooks/elastic/autoops"
echo
echo "Health check:"
echo "  http://${alb_dns}/healthz"
echo "=================================================="
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/12-print-url.sh
git add scripts/deploy/12-print-url.sh
git commit -m "SIO-789: phase D.3 print-url script"
```

---

## Task 18: Teardown script

**Files:**
- Create: `scripts/deploy/teardown.sh`

- [ ] **Step 1: Write the teardown script**

Create `scripts/deploy/teardown.sh`:

```bash
#!/usr/bin/env bash
# scripts/deploy/teardown.sh
# Deletes everything created by the deploy scripts, in reverse order.
# IMPORTANT: this WILL delete data. Use only in lab/dev.

SCRIPT_NAME="teardown"
source "$(dirname "$0")/lib.sh"

read -r -p "This will delete the entire eventgate AWS stack in $AWS_REGION. Type 'destroy' to confirm: " confirm
if [[ "$confirm" != "destroy" ]]; then
  die "aborted"
fi

cluster_arn="$(read_env ECS_CLUSTER_ARN)"

if [[ -n "$cluster_arn" ]]; then
  for svc in eventgate-gateway eventgate-writer; do
    if aws ecs describe-services --cluster "$cluster_arn" --services "$svc" \
       --query "services[?status=='ACTIVE']" --output text | grep -q .; then
      log "scaling $svc to 0 and deleting"
      aws ecs update-service --cluster "$cluster_arn" --service "$svc" --desired-count 0 >/dev/null || true
      aws ecs delete-service --cluster "$cluster_arn" --service "$svc" --force >/dev/null || true
    fi
  done
  aws ecs delete-cluster --cluster "$cluster_arn" >/dev/null || true
  log "deleted ECS cluster"
fi

listener_arn="$(read_env LISTENER_ARN)"
[[ -n "$listener_arn" ]] && aws elbv2 delete-listener --listener-arn "$listener_arn" 2>/dev/null || true

alb_arn="$(read_env ALB_ARN)"
[[ -n "$alb_arn" ]] && aws elbv2 delete-load-balancer --load-balancer-arn "$alb_arn" 2>/dev/null || true

tg_arn="$(read_env GATEWAY_TG_ARN)"
[[ -n "$tg_arn" ]] && aws elbv2 delete-target-group --target-group-arn "$tg_arn" 2>/dev/null || true

msk_arn="$(read_env MSK_CLUSTER_ARN)"
if [[ -n "$msk_arn" ]]; then
  log "deleting MSK cluster (this takes a few minutes)"
  aws kafka delete-cluster --cluster-arn "$msk_arn" || true
fi

for role in eventgate-gateway-task eventgate-writer-task; do
  aws iam delete-role-policy --role-name "$role" --policy-name eventgate-msk-access 2>/dev/null || true
  aws iam delete-role --role-name "$role" 2>/dev/null || true
done
aws iam detach-role-policy --role-name eventgate-task-execution \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy 2>/dev/null || true
aws iam delete-role --role-name eventgate-task-execution 2>/dev/null || true

# Log groups
for lg in /eventgate/gateway /eventgate/writer; do
  aws logs delete-log-group --log-group-name "$lg" 2>/dev/null || true
done

# ECR (must delete images first if any)
aws ecr delete-repository --repository-name eventgate --force 2>/dev/null || true

# Network — VPC, NAT, EIP, IGW, subnets, route tables
nat_id="$(read_env NAT_GW_ID)"
if [[ -n "$nat_id" ]]; then
  aws ec2 delete-nat-gateway --nat-gateway-id "$nat_id" 2>/dev/null || true
  log "waiting for NAT gateway to delete"
  aws ec2 wait nat-gateway-deleted --nat-gateway-ids "$nat_id" 2>/dev/null || true
fi
eip_id="$(read_env NAT_EIP_ID)"
[[ -n "$eip_id" ]] && aws ec2 release-address --allocation-id "$eip_id" 2>/dev/null || true

for sg in SG_ALB SG_GATEWAY SG_WRITER SG_MSK; do
  sgid="$(read_env "$sg")"
  [[ -n "$sgid" ]] && aws ec2 delete-security-group --group-id "$sgid" 2>/dev/null || true
done

vpc_id="$(read_env VPC_ID)"
igw_id="$(read_env IGW_ID)"
if [[ -n "$igw_id" && -n "$vpc_id" ]]; then
  aws ec2 detach-internet-gateway --internet-gateway-id "$igw_id" --vpc-id "$vpc_id" 2>/dev/null || true
  aws ec2 delete-internet-gateway --internet-gateway-id "$igw_id" 2>/dev/null || true
fi
for s in PUBLIC_SUBNET_A PUBLIC_SUBNET_B PRIVATE_SUBNET_A PRIVATE_SUBNET_B; do
  sid="$(read_env "$s")"
  [[ -n "$sid" ]] && aws ec2 delete-subnet --subnet-id "$sid" 2>/dev/null || true
done
for r in PUBLIC_RT_ID PRIVATE_RT_ID; do
  rid="$(read_env "$r")"
  [[ -n "$rid" ]] && aws ec2 delete-route-table --route-table-id "$rid" 2>/dev/null || true
done
[[ -n "$vpc_id" ]] && aws ec2 delete-vpc --vpc-id "$vpc_id" 2>/dev/null || true

log "teardown complete. .env.aws preserved for audit; delete manually if desired."
```

- [ ] **Step 2: Make it executable and commit**

```bash
chmod +x scripts/deploy/teardown.sh
git add scripts/deploy/teardown.sh
git commit -m "SIO-789: teardown script for the AWS stack"
```

---

## Task 19: Execute Phase A (one-time infra) and Phase B (build)

This task is interactive — it runs the deploy scripts against the live AWS account via the AWS MCP `call_aws` tool. The user must have AWS credentials configured.

**Files:** None modified; this provisions AWS resources.

- [ ] **Step 1: Verify the AWS MCP connector is authenticated**

Run: `aws sts get-caller-identity` (via AWS MCP `call_aws` tool).
Expected: returns `Account`, `UserId`, `Arn` for the target account. If it fails, the connector needs to be re-authenticated by the user.

- [ ] **Step 2: Run Phase A scripts in order**

Run from the worktree root:

```bash
cd scripts/deploy
./01-network.sh
./02-security-groups.sh
./03-msk.sh        # blocks ~10 min waiting for MSK ACTIVE
./04-ecr.sh
./05-ecs-cluster.sh
./06-log-groups.sh
./07-alb.sh
./08-iam-roles.sh
```

Expected: each script prints "done" and updates `scripts/deploy/.env.aws`. The MSK script will poll for ACTIVE state — leave it running.

If any script fails: read the error, fix the underlying issue (typically: wrong region, missing IAM perm, name collision), and re-run. All scripts are idempotent.

- [ ] **Step 3: Run Phase B (build and push image)**

Run:

```bash
./build-and-push.sh
```

Expected: image builds (`linux/amd64`), pushes both `:<sha>` and `:latest` tags to ECR. `IMAGE_URI` written to `.env.aws`.

- [ ] **Step 4: Run Phase C (create topics)**

Run:

```bash
cd $(git rev-parse --show-toplevel)
bun scripts/deploy/09-create-topics.ts
```

Expected: prints "created topics: ops.elastic.autoops.raw.v1, ops.elastic.autoops.events.v1, ops.elastic.autoops.dlq.v1" on first run, "all topics already exist" on subsequent runs.

- [ ] **Step 5: Run Phase D (register task defs + create services)**

```bash
cd scripts/deploy
./10-register-task-defs.sh
./11-deploy-services.sh
./12-print-url.sh
```

Expected: task defs registered, both services created and reach steady state (`runningCount=1, desiredCount=1`). The URL printer outputs the ALB DNS.

- [ ] **Step 6: Smoke test the public endpoint**

```bash
ALB_DNS=$(grep '^ALB_DNS=' scripts/deploy/.env.aws | cut -d= -f2)

# Healthcheck
curl -sf "http://${ALB_DNS}/healthz"
# Expected: 200, body like {"status":"ok"} or similar

# Send a sample AutoOps payload
curl -sf -X POST "http://${ALB_DNS}/webhooks/elastic/autoops" \
  -H 'Content-Type: application/json' \
  -d '{"resourceId":"r-aws-1","resourceName":"prod-search","title":"JVM memory pressure high","severity":"High","status":"open","startTime":"2026-05-18T19:27:40Z"}'
# Expected: 202, body { accepted: true, resourceId: "r-aws-1", idempotencyKey: "..." }
```

- [ ] **Step 7: Verify the writer logs the event**

```bash
aws logs tail /eventgate/writer --since 5m --follow
```

Expected: see a JSON log line within ~5 seconds with `"normalized event received (couchbase disabled)"`, `resourceId: "r-aws-1"`, and the matching `idempotencyKey`. Ctrl-C to stop.

- [ ] **Step 8: No commit needed**

This task does not modify files; it provisions infra. If `scripts/deploy/.env.aws` was created, confirm it's gitignored (it should be from Task 5):

```bash
git status scripts/deploy/.env.aws
# Expected: nothing — .env.aws is gitignored
```

---

## Task 20: Update README with the deploy section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read the current README**

Run: `head -60 README.md`
Note the existing section headings; the next step appends after them.

- [ ] **Step 2: Append the deploy section**

Append to `README.md`:

```markdown

## AWS deployment (v1)

eventgate ships with a set of manual deploy scripts under `scripts/deploy/` that
stand up the gateway + writer on ECS Fargate in `eu-central-1`, backed by MSK
Serverless. Couchbase is intentionally disabled in v1 — the writer logs
normalized events to CloudWatch Logs (`/eventgate/writer`).

See [`scripts/deploy/README.md`](scripts/deploy/README.md) for the full runbook.
The webhook URL is the ALB DNS name printed by `12-print-url.sh`.

To tear the stack down: `scripts/deploy/teardown.sh` (interactive confirmation).
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "SIO-789: document AWS deployment in README"
```

---

## Final verification

After all tasks complete:

- [ ] `bun run typecheck && bun test` passes.
- [ ] `curl http://<alb-dns>/healthz` returns 200.
- [ ] A POST to `http://<alb-dns>/webhooks/elastic/autoops` returns 202 within 1 second.
- [ ] CloudWatch Logs `/eventgate/writer` shows the normalized event within 5 seconds.
- [ ] Pasting the URL into AutoOps' webhook connector and clicking "Validate" succeeds.
- [ ] Local docker-compose dev flow (`docker compose up -d && bun run dev:gateway`) still works — no regression from the IAM SASL addition (defaults keep `KAFKA_AUTH=none`).
- [ ] Update the Linear issue [SIO-789](https://linear.app/siobytes/issue/SIO-789/deploy-eventgate-to-aws-fargate-v1-eu-central-1-no-couchbase) to "In Review", append a comment with the ALB DNS.
