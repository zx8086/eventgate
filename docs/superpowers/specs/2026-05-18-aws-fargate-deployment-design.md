# Design: Deploy eventgate to AWS Fargate (v1)

**Date:** 2026-05-18
**Region:** `eu-central-1`
**Status:** Approved by user, ready for implementation plan

## Goal

Stand up the existing `eventgate` repo on AWS so that Elastic AutoOps can POST webhooks to a public URL and the gateway → MSK → writer pipeline runs end-to-end. Couchbase is deferred — the writer just logs the normalized event.

Success: paste the ALB DNS into AutoOps' "Webhook URL" field, click "Test", see the normalized event in the writer's CloudWatch logs.

## Non-goals (v1)

- TLS / custom domain — HTTP only on the ALB. AutoOps accepts `http://`.
- Couchbase Capella — writer skips the cluster connect when `COUCHBASE_ENABLED=false`.
- CI/CD pipeline — manual deploy via the AWS MCP `call_aws` tool. Adding GitHub Actions is a separate ticket.
- Webhook auth — already out of scope per `CLAUDE.md`.
- OpenTelemetry — Phase 3, deferred.
- Autoscaling — fixed desired-count of 1 for both services in v1.
- Multi-AZ resilience for the writer — single task, single AZ.

## Architecture

```
AutoOps webhook
      │ HTTP
      ▼
ALB (eu-central-1, public, :80)
      │
      ▼
ECS Fargate service: gateway (Bun.serve :3000)
      │ produces (IAM SASL, :9098)
      ▼
MSK Serverless cluster (private, IAM auth)
      │ consumes (IAM SASL, :9098)
      ▼
ECS Fargate service: writer (logs to CloudWatch)
```

All in one VPC. ALB in public subnets, Fargate tasks + MSK in private subnets, NAT GW for outbound (ECR pulls, STS, CloudWatch).

## AWS resources

| # | Resource | Purpose |
|---|---|---|
| 1 | VPC `eventgate-vpc` (10.0.0.0/16) + 2 public subnets + 2 private subnets + IGW + 1 NAT GW + route tables | Network |
| 2 | Security groups: `sg-alb` (ingress :80 from 0.0.0.0/0), `sg-gateway` (ingress :3000 from sg-alb), `sg-writer` (no ingress), `sg-msk` (ingress :9098 from sg-gateway + sg-writer) | Network isolation |
| 3 | ECR repo `eventgate` | Container images, single repo for both services |
| 4 | MSK Serverless cluster `eventgate-msk` (IAM auth, in private subnets) | Kafka |
| 5 | ECS cluster `eventgate` (Fargate-only) | Compute home |
| 6 | Task definitions: `eventgate-gateway` (256 CPU / 512 MB), `eventgate-writer` (256 CPU / 512 MB) | Run specs |
| 7 | Task IAM roles ×2 with `kafka-cluster:Connect`, `WriteData`, `ReadData`, `DescribeTopic`, `CreateTopic`, `WriteDataIdempotently` on the MSK cluster ARN + topic ARNs + group ARNs, plus `logs:CreateLogStream`/`PutLogEvents` | Least-privilege per service |
| 8 | CloudWatch log groups `/eventgate/gateway`, `/eventgate/writer` (retention: 7 days) | Observability |
| 9 | ALB `eventgate-alb` (internet-facing) + listener :80 + target group on :3000 with health check `GET /healthz` | Public ingress |
| 10 | ECS services: `gateway` (1 task, attached to TG, public subnets disabled, in private subnets behind ALB) and `writer` (1 task, no LB, private subnets) | Run the apps |

## Code changes

Three changes inside the repo to make the same code run against MSK.

### 1. Kafka IAM SASL support

New env: `KAFKA_AUTH` (`none` | `iam`, default `none`). When `iam`:

- Add dep: `aws-msk-iam-sasl-signer-js`.
- In `src/kafka/producer.ts` and `src/kafka/consumer.ts`, configure `kafkajs` with:
  ```ts
  {
    ssl: true,
    sasl: {
      mechanism: "oauthbearer",
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region: config.kafka.region });
        return { value: token };
      },
    },
  }
  ```
- New config field `kafka.region` (string), required when `auth=iam`.

### 2. Config schema additions

- `kafka.auth: z.enum(["none", "iam"]).default("none")`
- `kafka.region: z.string().optional()` with `.describe()`
- New `.superRefine` rule: when `app.environment=prod`, require `kafka.auth=iam`.
- New `.superRefine` rule: when `kafka.auth=iam`, require `kafka.region`.

### 3. Couchbase gate

- New env: `COUCHBASE_ENABLED` (`true` | `false`, default `true`).
- `src/writer/index.ts`: if `false`, skip the `couchbase.connect` and the upserts. Log `{ normalized }` only.
- Config schema: `couchbase.enabled: z.boolean().default(true)`.
- `.superRefine` rule: when `couchbase.enabled=true` AND `app.environment=prod`, keep the existing prod-safety rules (no localhost, no default password, `couchbases://` required). When `enabled=false`, skip all Couchbase prod-safety refinements.

### 4. Dockerfile (new)

Multi-stage Bun image at repo root:

```dockerfile
FROM oven/bun:1-alpine AS deps
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM oven/bun:1-alpine
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json bun.lock ./
COPY src ./src
USER bun
# CMD is set per-service in the ECS task definition
```

One image, two services. Task definitions set `command: ["bun", "run", "src/gateway/index.ts"]` or `["bun", "run", "src/writer/index.ts"]`.

### 5. `.env.example` updates

Document `KAFKA_AUTH`, `KAFKA_REGION`, `COUCHBASE_ENABLED`, and an `.env.aws` example block.

## Deployment flow (manual, via AWS MCP)

All shell scripts under `scripts/deploy/`. Each script is idempotent (uses `aws ... --query` to check for existing resources first). Outputs written to `scripts/deploy/.env.aws` (gitignored) so later scripts can read ARNs without flag-juggling.

### Phase A — one-time infra

| Script | Creates | Notes |
|---|---|---|
| `01-network.sh` | VPC, subnets, IGW, NAT GW, route tables | ~3 min |
| `02-security-groups.sh` | All four SGs | Depends on VPC |
| `03-msk.sh` | MSK Serverless cluster | ~10 min to provision; kick off early |
| `04-ecr.sh` | ECR repo `eventgate` | Instant |
| `05-ecs-cluster.sh` | ECS cluster `eventgate` | Instant |
| `06-log-groups.sh` | Both CloudWatch log groups (7-day retention) | Instant |
| `07-alb.sh` | ALB + target group + listener | ~2 min |
| `08-iam-roles.sh` | Task execution role + two task roles with MSK perms scoped to the cluster ARN | Instant |

### Phase B — build & push image (every code change)

`scripts/deploy/build-and-push.sh`:
- `docker build -t eventgate:$(git rev-parse --short HEAD) .`
- `aws ecr get-login-password --region eu-central-1 | docker login ...`
- Tag + push.

### Phase C — pre-create Kafka topics (one-time, after MSK is up)

`scripts/deploy/09-topics.sh`:
- Use the MSK bootstrap broker DNS + IAM SASL via `kafka-topics.sh` from a Bun script, or shell out to a one-off Fargate task running `kafkajs` admin client. Simpler path: a tiny `scripts/deploy/create-topics.ts` Bun script that uses the same `kafkajs` + IAM SASL setup as the apps, run from a developer laptop with AWS creds.
- Creates: `ops.elastic.autoops.raw.v1`, `ops.elastic.autoops.events.v1`, `ops.elastic.autoops.dlq.v1`.

### Phase D — deploy apps (every code change)

| Script | Action |
|---|---|
| `10-register-task-defs.sh` | Register/update both task definitions with the new image tag |
| `11-deploy-services.sh` | First run: create `gateway` and `writer` ECS services. Subsequent runs: `aws ecs update-service --force-new-deployment` to roll the new task def |
| `12-print-url.sh` | Echo the ALB DNS as the webhook URL |

### Required developer-laptop prerequisites

- `aws` CLI v2 configured with credentials for the target account (env vars or `~/.aws/credentials`).
- `docker` for building images.
- The AWS MCP connector authenticated to the same account (the user has confirmed this is set up).

## Verification

1. Run Phase A scripts in order; wait for MSK `ACTIVE` (script polls).
2. Run Phase B to push first image.
3. Run Phase C to create topics.
4. Run Phase D; wait for both services to reach `runningCount=desiredCount=1`.
5. `curl http://<alb-dns>/healthz` → 200.
6. Send a sample AutoOps payload:
   ```bash
   curl -X POST http://<alb-dns>/webhooks/elastic/autoops \
     -H 'Content-Type: application/json' \
     -d '{"resourceId":"r-123","resourceName":"search-prod-eu","title":"JVM memory pressure high","severity":"High","status":"open","startTime":"2026-05-18T19:27:40Z"}'
   ```
   → 202 with `{ accepted: true, resourceId, idempotencyKey }`.
7. CloudWatch Logs Insights on `/eventgate/writer`:
   ```
   fields @timestamp, message, resourceId, alertSignature, idempotencyKey
   | filter component = "writer"
   | sort @timestamp desc
   | limit 20
   ```
   → see the normalized event logged.
8. AutoOps connector test: paste the ALB DNS as the webhook URL, click "Validate" in the AutoOps UI, expect green.

## Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| MSK Serverless minimum cost (~€500/mo just to keep it running) | High | Flagged to user. Tear down with `aws kafka delete-cluster` when not in use. Add a `teardown.sh` script. |
| IAM SASL token expiry (tokens are ~15 min) during long-running writer consumer | Medium | `aws-msk-iam-sasl-signer-js` + `kafkajs`'s `oauthBearerProvider` regenerate per connect; verify on long-running soak. |
| Auto-create topics off by default on MSK Serverless | High | Phase C pre-creates topics explicitly. |
| Public ALB on HTTP = unencrypted webhook payloads | Accepted for v1 | TODO ticket: add ACM + Route53 in v2. |
| ECR image pull permissions for Fargate | Medium | Phase A's task execution role gets `AmazonECSTaskExecutionRolePolicy`. |
| Couchbase guard regresses local dev (writer now skips Couchbase if `COUCHBASE_ENABLED=false`) | Low | Default stays `true` so local docker-compose flow is unchanged. |

## Out of scope (do not add)

- CDK / Terraform IaC (deferred; manual scripts for now).
- GitHub Actions pipeline.
- HTTPS / ACM / Route53.
- Couchbase Capella connection.
- Autoscaling policies on the ECS services.
- VPC endpoints for ECR/STS/Logs (would eliminate NAT cost; add when bill matters).
- WAF in front of the ALB.
- Multi-region failover.

## Related code references

- `src/kafka/producer.ts` — point of change for IAM SASL.
- `src/kafka/consumer.ts` — same.
- `src/config/schemas.ts` — add `kafka.auth`, `kafka.region`, `couchbase.enabled`.
- `src/config/defaults.ts` — sensible defaults for the new fields.
- `src/config/envMapping.ts` — wire `KAFKA_AUTH`, `KAFKA_REGION`, `COUCHBASE_ENABLED`.
- `src/writer/index.ts` — Couchbase-enabled guard.
- `Dockerfile` — new file at repo root.
- `scripts/deploy/` — new directory, numbered shell scripts + one Bun topic-creation script.
- `.env.example` — document new env vars and the AWS deployment block.

## Linear

Create a Linear issue in the [Event Gate](https://linear.app/siobytes/project/event-gate-9bf5601b0c39/overview) project with this design's TL;DR and link to this file.
