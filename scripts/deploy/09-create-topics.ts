// scripts/deploy/09-create-topics.ts
// Pre-creates the three eventgate topics on the Redpanda broker.
// Redpanda's user-data already creates these on first boot; this script is a
// safety net for re-runs or environments where auto-create is disabled.
// Run from inside the VPC (e.g. as a one-shot Fargate task): bun scripts/deploy/09-create-topics.ts
import { Admin } from "@platformatic/kafka";
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

const bootstrap = env.KAFKA_BROKERS;
if (!bootstrap) throw new Error("KAFKA_BROKERS missing in .env.aws");

const admin = new Admin({
  clientId: "eventgate-topic-bootstrap",
  bootstrapBrokers: bootstrap.split(","),
});

const desired = [
  "ops.elastic.autoops.raw.v1",
  "ops.elastic.autoops.events.v1",
  "ops.elastic.autoops.dlq.v1",
];

try {
  await admin.createTopics({ topics: desired, partitions: 3, replicas: 1 });
  console.log(`created topics: ${desired.join(", ")}`);
} catch (err) {
  console.log("createTopics returned (may be no-op if topics exist):", err);
}

await admin.close();
