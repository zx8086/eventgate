# AWS ECS Fargate Deployment

> **Targets:** Bun 1.3.11+ | TypeScript 5.x | AWS ECS Fargate
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate deploys as one container image with two ECS task definitions and two services — gateway and writer. The split happens at the ECS task-definition layer by overriding the container `command`; the image itself defaults to launching the gateway. This document describes the deployed shape, the scripts that produce it, and what each script owns. For the image build itself see [container-image.md](container-image.md).

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
|  +-----------------+   +-----------------+   |
|  | eventgate-      |   | eventgate-      |   |
|  | gateway service |   | writer service  |   |
|  |  desired = 1    |   |  desired = 1    |   |
|  |  sg = $sg_gw    |   |  sg = $sg_wr    |   |
|  |  port 3000      |   |  no port        |   |
|  +--------+--------+   +--------+--------+   |
|           |                     |            |
+-----------|---------------------|------------+
            |                     |
            | produce             | consume + DLQ
            v                     v
        +----------------------------+
        |  MSK Serverless            |
        |  IAM SASL                  |
        |  raw.v1, events.v1, dlq.v1 |
        +----------------------------+

        +----------------------------+
        |  Couchbase Capella         |
        |  (optional in v1 - see     |
        |   COUCHBASE_ENABLED)       |
        +----------------------------+
```

## One Image, Two Task Definitions

| Aspect | gateway | writer |
|--------|---------|--------|
| Image | `$IMAGE_URI` (same as writer) | `$IMAGE_URI` (same as gateway) |
| Container `command` | `["bun","run","src/gateway/index.ts"]` | `["bun","run","src/writer/index.ts"]` |
| Port mapping | `3000/tcp` | none |
| Log group | `/eventgate/gateway` | `/eventgate/writer` |
| Task role | `$GATEWAY_TASK_ROLE_ARN` | `$WRITER_TASK_ROLE_ARN` |
| ALB attachment | target group `$GATEWAY_TG_ARN`, container port 3000 | none |
| Security group | `$SG_GATEWAY` (ingress from ALB) | `$SG_WRITER` (egress to MSK + Couchbase) |
| CPU / memory | 256 / 512 | 256 / 512 |
| Healthcheck | gateway image healthcheck (`/healthz`) | container healthcheck disabled |

The `command` override is registered by `scripts/deploy/10-register-task-defs.sh`. Without an override, the container would default to the gateway entry point baked into the image — useful but easy to forget, so the writer task definition always sets `command` explicitly.

## Deployment Pipeline

The image is built and pushed by `.github/workflows/cd.yml` on pushes to `main`/`master` and version tags. Infrastructure and service deployment are still operator-driven — the scripts under `scripts/deploy/` are idempotent and intended to be re-run safely.

| Phase | Script | What it owns |
|-------|--------|--------------|
| A.1 | `01-network.sh` | VPC, subnets, IGW, NAT |
| A.2 | `02-security-groups.sh` | All security groups (ALB, gateway, writer, MSK) |
| A.3 | `03-msk.sh` | MSK Serverless cluster (~10 min) |
| A.4 | `04-ecr.sh` | ECR repository (currently unused — image lives on Docker Hub) |
| A.5 | `05-ecs-cluster.sh` | ECS cluster |
| A.6 | `06-log-groups.sh` | `/eventgate/gateway`, `/eventgate/writer` CloudWatch log groups |
| A.7 | `07-alb.sh` | ALB, target group, listener |
| A.8 | `08-iam-roles.sh` | Task execution role + per-process task roles |
| B | `build-and-push.sh` | Local image build + push (alternative to the CD workflow) |
| C | `09-create-topics.ts` | Pre-creates `raw.v1`, `events.v1`, `dlq.v1` on MSK |
| D.1 | `10-register-task-defs.sh` | Registers `eventgate-gateway` and `eventgate-writer` task definitions |
| D.2 | `11-deploy-services.sh` | Creates or updates the two ECS services; waits for stable |
| D.3 | `12-print-url.sh` | Prints the ALB DNS name |
| Cleanup | `teardown.sh` | Reverses every phase (interactive confirmation) |

The scripts share state through `scripts/deploy/.env.aws` (gitignored). See [`../../scripts/deploy/README.md`](../../scripts/deploy/README.md) for the operator runbook.

## Service-level Behaviour

### Gateway service

`scripts/deploy/11-deploy-services.sh` creates the gateway service with:

- `desired-count = 1`
- `launch-type = FARGATE`
- `network-configuration` in private subnets, security group `$SG_GATEWAY`, `assignPublicIp=DISABLED`
- ALB target group attached at `containerName=eventgate-gateway, containerPort=3000`
- `health-check-grace-period-seconds = 60`

The gateway is reachable only through the ALB. The ALB DNS name is the URL the AutoOps connector POSTs to.

### Writer service

Same script creates the writer service with:

- `desired-count = 1`
- `launch-type = FARGATE`
- `network-configuration` in private subnets, security group `$SG_WRITER`, `assignPublicIp=DISABLED`
- No load balancer, no port mapping

The writer reaches MSK and (when enabled) Couchbase Capella outbound through its security group. It has no inbound listeners.

## Environment Block

The task definitions inject the same environment block on both containers:

```bash
ENVIRONMENT=prod
KAFKA_AUTH=iam
KAFKA_REGION=<aws-region>
KAFKA_BROKERS=<msk-bootstrap-string>
COUCHBASE_ENABLED=false
LOG_LEVEL=info
```

Per-variable detail and production safety refinements live in [../configuration/environment-variables.md](../configuration/environment-variables.md). Of note:

- `ENVIRONMENT=prod` triggers the `.superRefine()` block in `src/config/schemas.ts` — IAM SASL required, `localhost` brokers rejected, default Couchbase password rejected.
- `COUCHBASE_ENABLED=false` lets the writer deploy before Capella is ready; the writer logs events instead of upserting.

When Capella is provisioned, replace the writer's `COUCHBASE_ENABLED` with the TLS-prefixed connection string and provide the credentials via the task role's Secrets Manager binding (not in the inline `environment` block).

## Scaling

The two services are independent on purpose:

| Service | Scale on | Why |
|---------|----------|-----|
| gateway | ALB request count or CPU | Request-bound — capacity tracks inbound webhook rate |
| writer | MSK consumer-group lag or CPU | Lag-bound — capacity tracks event throughput, not request rate |

If gateway and writer shared a task definition, they would be forced to scale together and waste capacity on whichever side was not the bottleneck.

## Deployment Verification

After `11-deploy-services.sh` completes, `aws ecs wait services-stable` has already confirmed both services reached stable state. Additional checks worth running:

1. `aws ecs describe-services --cluster <cluster> --services eventgate-gateway eventgate-writer --query 'services[].{name:serviceName,desired:desiredCount,running:runningCount,deployments:deployments[].rolloutState}'` — both `runningCount == desiredCount`, all `rolloutState == COMPLETED`.
2. `curl https://<alb-dns>/healthz` — `200 {"ok":true}`.
3. CloudWatch Logs: `/eventgate/gateway` and `/eventgate/writer` both have a recent `service.name=eventgate` ECS-NDJSON line announcing the listener / consumer.
4. Send a test webhook (see [../api/webhooks.md](../api/webhooks.md)) and confirm a corresponding `autoops.event.received` line appears in the gateway log group within seconds.

## Rollback

The task definition is the rollback unit. To revert:

```bash
aws ecs update-service \
  --cluster $ECS_CLUSTER_ARN \
  --service eventgate-gateway \
  --task-definition eventgate-gateway:<previous-revision> \
  --force-new-deployment

aws ecs update-service \
  --cluster $ECS_CLUSTER_ARN \
  --service eventgate-writer \
  --task-definition eventgate-writer:<previous-revision> \
  --force-new-deployment
```

Both services then run the older image and environment block within a single ECS deployment.

## See Also

- [container-image.md](container-image.md) — how the image is built (tiered Dockerfile, build args, healthcheck).
- [../architecture/overview.md](../architecture/overview.md) — why the two-process split exists.
- [../configuration/environment-variables.md](../configuration/environment-variables.md) — production safety refinements.
- [../../scripts/deploy/README.md](../../scripts/deploy/README.md) — operator runbook.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial AWS ECS deployment doc created |
