// scripts/deploy/09-create-topics.ts
// Pre-creates each route's Kafka topic on the target broker. Topics are read
// from the same source the runtime gateway uses, so this stays in sync with
// `src/config/defaults.ts` automatically — no hardcoded list to drift.
// Run from inside the VPC (e.g. as a one-shot Fargate task): bun scripts/deploy/09-create-topics.ts
import { Admin } from "@platformatic/kafka";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { defaults } from "../../src/config/defaults.ts";

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

const bootstrap = env.KAFKA_BROKERS;
if (!bootstrap) throw new Error("KAFKA_BROKERS missing in .env.aws");

const admin = new Admin({
  clientId: "eventgate-topic-bootstrap",
  bootstrapBrokers: bootstrap.split(","),
});

// Routes declared in ROUTES_JSON override defaults at deploy time. Mirror the
// gateway's precedence (ROUTES_JSON > defaults) so operators can pre-create
// vendor topics without editing the seed file.
const routesJson = env.ROUTES_JSON;
const routes = routesJson
  ? (JSON.parse(routesJson) as Array<{ topic: string }>)
  : defaults.routes;

const desired = [...new Set(routes.map((r) => r.topic))];
if (desired.length === 0) throw new Error("no routes resolved; nothing to create");

try {
  await admin.createTopics({ topics: desired, partitions: 3, replicas: 1 });
  console.log(`created topics: ${desired.join(", ")}`);
} catch (err) {
  console.log("createTopics returned (may be no-op if topics exist):", err);
}

await admin.close();
