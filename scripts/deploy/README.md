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
