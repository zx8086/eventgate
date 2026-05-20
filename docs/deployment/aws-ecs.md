# AWS ECS Fargate Deployment

> **Targets:** Bun 1.3.11+ | TypeScript 5.x | AWS ECS Fargate
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate deploys as a single container image and a single ECS service: the gateway. There is no second process in this repo. The image defaults to `CMD ["bun","src/gateway/index.ts"]`, so no command override is needed. For the image build itself see [container-image.md](container-image.md). For Kafka provider selection see [../architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md).

> **Scope note (SIO-795):** the deploy scripts under `scripts/deploy/` still provision the older two-service shape (gateway + writer + Couchbase env vars). They are out of scope for the SIO-795 PR and tracked separately. This document describes the **target** deployment shape — code today; scripts next.

## Deployment Topology

```
                +-----------------+
                |  Internet       |
                +--------+--------+
                         |
                         v
                +-----------------+
                |  ALB            |
                |  (public)       |
                +--------+--------+
                         | HTTP :3000
                         v
+----------------------------------------------+
|  ECS Fargate cluster                         |
|                                              |
|  +-----------------+                         |
|  | eventgate-      |                         |
|  | gateway service |                         |
|  |  desired = 1+   |                         |
|  |  sg = $sg_gw    |                         |
|  |  port 3000      |                         |
|  +--------+--------+                         |
|           |                                  |
+-----------|----------------------------------+
            |
            | produce (SASL/OAUTHBEARER + TLS)
            v
        +-----------------------------------+
        |  MSK Serverless                   |
        |  raw.v1  (gateway writes here)    |
        |  events.v1, dlq.v1 (provisioned;  |
        |  reserved for future consumers)   |
        +-----------------------------------+
```

## Single Task Definition

| Aspect | gateway |
|--------|---------|
| Image | `$IMAGE_URI` |
| Container `command` | (none — image default `bun src/gateway/index.ts`) |
| Port mapping | `3000/tcp` |
| Log group | `/eventgate/gateway` |
| Task role | `$GATEWAY_TASK_ROLE_ARN` — must grant `kafka:GetBootstrapBrokers` if `MSK_CLUSTER_ARN` is used, plus the MSK IAM auth actions: `kafka-cluster:Connect` and `kafka-cluster:DescribeTopic` for the cluster, and `kafka-cluster:WriteData` scoped to the `raw.v1` topic only (the gateway does not write `events.v1` or `dlq.v1`) |
| ALB attachment | target group `$GATEWAY_TG_ARN`, container port 3000 |
| Security group | `$SG_GATEWAY` (ingress from ALB; egress to MSK) |
| CPU / memory | 256 / 512 (baseline; scale on request count or CPU) |
| Healthcheck | gateway image healthcheck (`/healthz`) |

## Deployment Pipeline (target shape)

The image is built and pushed by `.github/workflows/cd.yml` on pushes to `main`/`master` and version tags. Infrastructure deployment is operator-driven.

| Phase | Script | What it owns |
|-------|--------|--------------|
| A.1 | `01-network.sh` | VPC, subnets, IGW, NAT |
| A.2 | `02-security-groups.sh` | ALB and gateway security groups |
| A.3 | `03-msk.sh` | MSK Serverless cluster |
| A.4 | `04-ecr.sh` | ECR repository (currently unused — image lives on Docker Hub) |
| A.5 | `05-ecs-cluster.sh` | ECS cluster |
| A.6 | `06-log-groups.sh` | `/eventgate/gateway` CloudWatch log group |
| A.7 | `07-alb.sh` | ALB, target group, listener |
| A.8 | `08-iam-roles.sh` | Task execution role + gateway task role (with MSK IAM permissions) |
| B | `build-and-push.sh` | Local image build + push (alternative to the CD workflow) |
| C | `09-create-topics.ts` | Pre-creates `raw.v1` (gateway sink), `events.v1`, `dlq.v1` (reserved for future consumers) on MSK |
| D.1 | `10-register-task-defs.sh` | Registers `eventgate-gateway` task definition |
| D.2 | `11-deploy-services.sh` | Creates or updates the gateway ECS service |
| D.3 | `12-print-url.sh` | Prints the ALB DNS name |
| Cleanup | `teardown.sh` | Reverses every phase |

## Environment Block

The task definition injects:

```bash
ENVIRONMENT=prod
KAFKA_PROVIDER=msk
KAFKA_CLIENT_ID=eventgate-gateway
MSK_REGION=<aws-region>
MSK_AUTH_MODE=iam
MSK_CLUSTER_ARN=<arn>          # or MSK_BROKERS=<csv>
OUTBOX_DB_PATH=/data/outbox.db
ROUTES_JSON=[ ... ]            # see "Routes" section below
LOG_LEVEL=info
```

Or for Confluent Cloud (alternative):

```bash
ENVIRONMENT=prod
KAFKA_PROVIDER=confluent
KAFKA_CLIENT_ID=eventgate-gateway
CONFLUENT_BOOTSTRAP_SERVERS=<host:port>
OUTBOX_DB_PATH=/data/outbox.db
ROUTES_JSON=[ ... ]            # see "Routes" section below
LOG_LEVEL=info
```

`CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET` should be injected from Secrets Manager via the task definition `secrets` block, not inline.

Per-variable detail and refinements live in [../configuration/environment-variables.md](../configuration/environment-variables.md). `ENVIRONMENT=prod` triggers the `.superRefine()` block in `src/config/schemas.ts` — `KAFKA_PROVIDER=local` is rejected, MSK requires region + brokers-or-arn, Confluent requires the full triplet.

## Routes — single or multiple vendors

Routes are baked into the task definition via `ROUTES_JSON`. Each entry pairs an HTTP path with its destination Kafka topic and partition-key strategy. The gateway listens on every declared path and publishes to that route's `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` topic. To onboard a new vendor: append an object to the array and deploy a new task definition revision. Route changes are deliberately **deploy-time only** on Fargate — the admin endpoint (`PUT /admin/routes` + `ROUTES_FILE`) is not used in this deployment shape because routes change rarely (~once a quarter) and the simpler single-mount setup is worth the brief gap of a rolling deploy.

Single vendor (Elastic AutoOps only):

```json
ROUTES_JSON=[
  {
    "name": "elastic-autoops",
    "path": "/webhooks/elastic/autoops",
    "topic": "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    "dlqTopic": "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    "keyFields": ["resourceId", "deployment-id"],
    "idempotency": "elastic-autoops"
  }
]
```

Multiple vendors (Elastic + Datadog + GitHub):

```json
ROUTES_JSON=[
  {
    "name": "elastic-autoops",
    "path": "/webhooks/elastic/autoops",
    "topic": "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    "dlqTopic": "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    "keyFields": ["resourceId", "deployment-id"],
    "idempotency": "elastic-autoops"
  },
  {
    "name": "datadog-alerts",
    "path": "/webhooks/datadog/alerts",
    "topic": "T_PRIVATE_SOURCE_DATADOG_ALERTS",
    "dlqTopic": "DLQ_T_PRIVATE_SOURCE_DATADOG_ALERTS",
    "keyFields": ["alert_id"]
  },
  {
    "name": "github-webhooks",
    "path": "/webhooks/github/events",
    "topic": "T_PRIVATE_SOURCE_GITHUB_EVENTS",
    "dlqTopic": "DLQ_T_PRIVATE_SOURCE_GITHUB_EVENTS",
    "keyFields": ["repository.full_name", "delivery"]
  }
]
```

The route schema (in `src/config/schemas.ts`) enforces every contract at startup: `topic` must match `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>`, `dlqTopic` (when present) must equal `DLQ_T_<topic>`, `path` must not collide with reserved paths (`/healthz`, `/admin/routes`) or other routes, `idempotency` must reference a registered strategy. Malformed `ROUTES_JSON` crashes the task on boot — ECS rolls back automatically.

A copy-pasteable task definition that ties this together with the EBS volume mount, IAM, log driver, and healthcheck lives in [task-definition-example.md](task-definition-example.md).

## Scaling

Scale on ALB request count or CPU — capacity tracks the inbound webhook rate. No second process exists to scale independently.

## Deployment Verification

After `11-deploy-services.sh` completes, `aws ecs wait services-stable` has confirmed the service reached stable state. Additional checks worth running:

1. `aws ecs describe-services --cluster <cluster> --services eventgate-gateway --query 'services[].{name:serviceName,desired:desiredCount,running:runningCount,deployments:deployments[].rolloutState}'` — `runningCount == desiredCount`, `rolloutState == COMPLETED`.
2. `curl https://<alb-dns>/healthz` — `200 {"ok":true}`.
3. CloudWatch Logs `/eventgate/gateway` shows `kafka provider selected` with the expected `provider` and `providerType`, followed by `gateway listening`.
4. Send a test webhook (see [../api/webhooks.md](../api/webhooks.md)) and confirm a record lands on `raw.v1` within seconds (the gateway does not log per successful request — verify via the Kafka topic or the outbox `pending` counter on `/healthz`).

## Rollback

The task definition is the rollback unit. To revert:

```bash
aws ecs update-service \
  --cluster $ECS_CLUSTER_ARN \
  --service eventgate-gateway \
  --task-definition eventgate-gateway:<previous-revision> \
  --force-new-deployment
```

## See Also

- [container-image.md](container-image.md) — how the image is built (tiered Dockerfile, build args, healthcheck).
- [../architecture/overview.md](../architecture/overview.md) — gateway-only architecture.
- [../architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md) — env vars for selecting MSK / Confluent / local.
- [../configuration/environment-variables.md](../configuration/environment-variables.md) — production safety refinements.
- [../../scripts/deploy/README.md](../../scripts/deploy/README.md) — operator runbook (pending update to drop the writer service).

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial AWS ECS deployment doc created |
| 2026-05-19 | Rewritten for single-service deployment; removed writer task def, replaced Couchbase env block with Kafka provider env block (SIO-795) |
| 2026-05-19 | Narrowed `kafka-cluster:WriteData` to `raw.v1`; updated verification step; clarified `events.v1`/`dlq.v1` are provisioned but unused by this service (SIO-801) |
