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
