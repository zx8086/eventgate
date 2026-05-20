# ECS Task Definition Example

> **Targets:** Bun 1.3.11+ | TypeScript 5.x | AWS ECS Fargate (platform version 1.4.0+)
> **Last updated:** 2026-05-20
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

Copy-pasteable task definition, service, and IaC snippets for the eventgate gateway on AWS ECS Fargate. Companion to [aws-ecs.md](aws-ecs.md) (deployment topology) and [container-image.md](container-image.md) (image build).

The Fargate shape is: **one task definition, one service, one EBS volume per task** for the SQLite outbox. Routes are baked into `ROUTES_JSON`. No EFS, no admin endpoint, no second mount. Route changes ship as new task definition revisions.

## Full MSK + IAM Task Definition

```json
{
  "family": "eventgate-gateway",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "arn:aws:iam::123456789012:role/eventgate-execution-role",
  "taskRoleArn": "arn:aws:iam::123456789012:role/eventgate-gateway-task-role",
  "runtimePlatform": {
    "operatingSystemFamily": "LINUX",
    "cpuArchitecture": "ARM64"
  },
  "volumes": [
    {
      "name": "outbox-data",
      "configuredAtLaunch": true
    }
  ],
  "containerDefinitions": [
    {
      "name": "gateway",
      "image": "123456789012.dkr.ecr.eu-central-1.amazonaws.com/eventgate-gateway:latest",
      "essential": true,
      "portMappings": [
        { "containerPort": 3000, "protocol": "tcp" }
      ],
      "mountPoints": [
        {
          "sourceVolume": "outbox-data",
          "containerPath": "/data",
          "readOnly": false
        }
      ],
      "environment": [
        { "name": "ENVIRONMENT",     "value": "prod" },
        { "name": "PORT",            "value": "3000" },
        { "name": "LOG_LEVEL",       "value": "info" },
        { "name": "KAFKA_PROVIDER",  "value": "msk" },
        { "name": "KAFKA_CLIENT_ID", "value": "eventgate-gateway" },
        { "name": "MSK_REGION",      "value": "eu-central-1" },
        { "name": "MSK_AUTH_MODE",   "value": "iam" },
        { "name": "MSK_CLUSTER_ARN", "value": "arn:aws:kafka:eu-central-1:123456789012:cluster/eventgate-msk/abcd1234-5678-90ab-cdef-1234567890ab-1" },
        { "name": "OUTBOX_DB_PATH",  "value": "/data/outbox.db" },
        {
          "name": "ROUTES_JSON",
          "value": "[{\"name\":\"elastic-autoops\",\"path\":\"/webhooks/elastic/autoops\",\"topic\":\"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS\",\"dlqTopic\":\"DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS\",\"keyFields\":[\"resourceId\",\"deployment-id\"],\"idempotency\":\"elastic-autoops\"}]"
        }
      ],
      "secrets": [],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/eventgate/gateway",
          "awslogs-region": "eu-central-1",
          "awslogs-stream-prefix": "gateway"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -fsS http://localhost:3000/healthz || exit 1"],
        "interval": 15,
        "timeout": 5,
        "retries": 3,
        "startPeriod": 30
      },
      "stopTimeout": 120,
      "readonlyRootFilesystem": true,
      "user": "1000:1000"
    }
  ]
}
```

Key points:

- `volumes[].configuredAtLaunch: true` is what enables per-task EBS attachment. Paired with `volumeConfigurations` on the service (below), ECS creates a fresh gp3 volume each time a task launches.
- `OUTBOX_DB_PATH=/data/outbox.db` points the SQLite outbox at the EBS mount. WAL sidecars (`outbox.db-wal`, `outbox.db-shm`) live in the same directory; nothing else writes to `/data`.
- `readonlyRootFilesystem: true` is safe because `/data` is a writable mount. The image itself stays immutable at runtime.
- `stopTimeout: 120` (seconds) gives the outbox drainer time to flush in-flight batches and `closeOutbox()` to checkpoint WAL on SIGTERM. Default 30s is too short for a drained shutdown.
- `healthCheck` uses `curl` against `/healthz` — the image already includes curl in both Tier 1 and Tier 2 variants per [container-image.md](container-image.md).
- `ROUTES_JSON` is a single escaped JSON string on one line; multi-line examples in [aws-ecs.md](aws-ecs.md) collapse to this form in raw JSON. CloudFormation and Terraform variants below avoid the manual escaping.
- `KAFKA_BROKERS` is omitted in this MSK example. With `MSK_CLUSTER_ARN` set, the provider calls `GetBootstrapBrokersCommand` at startup and resolves brokers automatically. To skip discovery (e.g. for stable broker pinning), set `KAFKA_BROKERS=b-1.foo.kafka-serverless.eu-central-1.amazonaws.com:9098,b-2...:9098` and the discovery call is bypassed.

## Multi-vendor `ROUTES_JSON`

Same task definition; only the `ROUTES_JSON` env entry changes:

```json
{
  "name": "ROUTES_JSON",
  "value": "[{\"name\":\"elastic-autoops\",\"path\":\"/webhooks/elastic/autoops\",\"topic\":\"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS\",\"dlqTopic\":\"DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS\",\"keyFields\":[\"resourceId\",\"deployment-id\"],\"idempotency\":\"elastic-autoops\"},{\"name\":\"datadog-alerts\",\"path\":\"/webhooks/datadog/alerts\",\"topic\":\"T_PRIVATE_SOURCE_DATADOG_ALERTS\",\"dlqTopic\":\"DLQ_T_PRIVATE_SOURCE_DATADOG_ALERTS\",\"keyFields\":[\"alert_id\"]},{\"name\":\"github-webhooks\",\"path\":\"/webhooks/github/events\",\"topic\":\"T_PRIVATE_SOURCE_GITHUB_EVENTS\",\"dlqTopic\":\"DLQ_T_PRIVATE_SOURCE_GITHUB_EVENTS\",\"keyFields\":[\"repository.full_name\",\"delivery\"]}]"
}
```

## Confluent Cloud Variant

Drop the MSK env block; add Confluent. Credentials come from Secrets Manager rather than inline.

```json
"environment": [
  { "name": "ENVIRONMENT",                  "value": "prod" },
  { "name": "PORT",                         "value": "3000" },
  { "name": "LOG_LEVEL",                    "value": "info" },
  { "name": "KAFKA_PROVIDER",               "value": "confluent" },
  { "name": "KAFKA_CLIENT_ID",              "value": "eventgate-gateway" },
  { "name": "KAFKA_BROKERS",                "value": "pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092" },
  { "name": "OUTBOX_DB_PATH",               "value": "/data/outbox.db" },
  { "name": "ROUTES_JSON",                  "value": "[...]" }
],
"secrets": [
  {
    "name": "CONFLUENT_API_KEY",
    "valueFrom": "arn:aws:secretsmanager:eu-central-1:123456789012:secret:eventgate/confluent-api-key-AbCdEf"
  },
  {
    "name": "CONFLUENT_API_SECRET",
    "valueFrom": "arn:aws:secretsmanager:eu-central-1:123456789012:secret:eventgate/confluent-api-secret-AbCdEf"
  }
]
```

The execution role needs `secretsmanager:GetSecretValue` on both secret ARNs; the task role does not (the SDK reads them via container injection, not a runtime API call).

## Matching ECS Service

The task definition only declares the volume; the service tells ECS how to attach it. Without `volumeConfigurations` the volume is treated as an ephemeral Docker volume on the host (Fargate-managed disk), and dies with the task.

```json
{
  "serviceName": "eventgate-gateway",
  "cluster": "eventgate-cluster",
  "taskDefinition": "eventgate-gateway",
  "desiredCount": 1,
  "launchType": "FARGATE",
  "platformVersion": "1.4.0",
  "deploymentConfiguration": {
    "minimumHealthyPercent": 0,
    "maximumPercent": 100
  },
  "networkConfiguration": {
    "awsvpcConfiguration": {
      "subnets": ["subnet-aaa", "subnet-bbb"],
      "securityGroups": ["sg-eventgate-gateway"],
      "assignPublicIp": "DISABLED"
    }
  },
  "loadBalancers": [
    {
      "targetGroupArn": "arn:aws:elasticloadbalancing:eu-central-1:123456789012:targetgroup/eventgate-gw/abc123",
      "containerName": "gateway",
      "containerPort": 3000
    }
  ],
  "volumeConfigurations": [
    {
      "name": "outbox-data",
      "managedEBSVolume": {
        "roleArn": "arn:aws:iam::123456789012:role/ecsInfrastructureRole",
        "filesystemType": "ext4",
        "sizeInGiB": 10,
        "volumeType": "gp3",
        "encrypted": true,
        "tagSpecifications": [
          {
            "resourceType": "volume",
            "tags": [{ "key": "Service", "value": "eventgate-gateway" }]
          }
        ]
      }
    }
  ]
}
```

Key points:

- **`minimumHealthyPercent: 0, maximumPercent: 100`** forces sequential deploys: stop old task → detach EBS → attach to new task → start. The default (`100/200`) tries to launch the new task before stopping the old one, and EBS attachment to a single task prevents that — deploys stall.
- **`desiredCount: 1`**. The SQLite outbox is single-writer per file. For HA, run **two independent services** in different AZs, each with its own EBS volume and outbox DB, behind the same ALB. Downstream consumers dedupe on the opportunistic `idempotencyKey` Kafka header.
- **`ecsInfrastructureRole`** is the role ECS assumes to manage EBS volume lifecycle (create, attach, detach, delete). It's separate from the task and execution roles. AWS docs call it the "infrastructure IAM role"; it needs `ec2:CreateVolume`, `ec2:AttachVolume`, `ec2:DetachVolume`, `ec2:DescribeVolumes`, `ec2:DeleteVolume`, plus tagging permissions. See the [AWS docs on EBS volumes for Fargate](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ebs-volumes.html).
- **Healthcheck.** The task definition's container healthcheck (above) plus the ALB target group healthcheck (`/healthz`, HTTP 200) together give ECS the signal to register/deregister tasks.

## CloudFormation Equivalent (YAML)

YAML's `|-` block scalar avoids manual escaping of the routes JSON:

```yaml
ContainerDefinitions:
  - Name: gateway
    Image: !Sub "${EcrRepo}:${ImageTag}"
    Environment:
      - { Name: ENVIRONMENT,     Value: prod }
      - { Name: KAFKA_PROVIDER,  Value: msk }
      - { Name: MSK_REGION,      Value: !Ref AwsRegion }
      - { Name: MSK_CLUSTER_ARN, Value: !Ref MskClusterArn }
      - { Name: MSK_AUTH_MODE,   Value: iam }
      - { Name: OUTBOX_DB_PATH,  Value: /data/outbox.db }
      - Name: ROUTES_JSON
        Value: !Sub |-
          [{"name":"elastic-autoops","path":"/webhooks/elastic/autoops","topic":"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS","dlqTopic":"DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS","keyFields":["resourceId","deployment-id"],"idempotency":"elastic-autoops"}]
    MountPoints:
      - SourceVolume: outbox-data
        ContainerPath: /data
        ReadOnly: false
    PortMappings:
      - { ContainerPort: 3000, Protocol: tcp }
```

For multi-vendor routes in CloudFormation, store the JSON in a `String` parameter or `Mappings` block to keep templates diff-friendly.

## Terraform Equivalent

Terraform's `jsonencode()` produces clean, diff-friendly output from a typed list variable. New vendor = new entry in `var.routes`; `terraform apply` produces a new task definition revision.

```hcl
resource "aws_ecs_task_definition" "gateway" {
  family                   = "eventgate-gateway"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.gateway_task.arn

  volume {
    name                = "outbox-data"
    configure_at_launch = true
  }

  container_definitions = jsonencode([
    {
      name         = "gateway"
      image        = "${aws_ecr_repository.gateway.repository_url}:latest"
      essential    = true
      portMappings = [{ containerPort = 3000, protocol = "tcp" }]
      mountPoints  = [{ sourceVolume = "outbox-data", containerPath = "/data", readOnly = false }]

      environment = [
        { name = "ENVIRONMENT",     value = "prod" },
        { name = "KAFKA_PROVIDER",  value = "msk" },
        { name = "MSK_REGION",      value = var.aws_region },
        { name = "MSK_AUTH_MODE",   value = "iam" },
        { name = "MSK_CLUSTER_ARN", value = var.msk_cluster_arn },
        { name = "OUTBOX_DB_PATH",  value = "/data/outbox.db" },
        { name = "ROUTES_JSON",     value = jsonencode(var.routes) }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/eventgate/gateway"
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "gateway"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -fsS http://localhost:3000/healthz || exit 1"]
        interval    = 15
        timeout     = 5
        retries     = 3
        startPeriod = 30
      }

      stopTimeout            = 120
      readonlyRootFilesystem = true
      user                   = "1000:1000"
    }
  ])
}

variable "routes" {
  type = list(object({
    name        = string
    path        = string
    topic       = string
    dlqTopic    = optional(string)
    keyFields   = list(string)
    idempotency = optional(string)
  }))
  default = [
    {
      name        = "elastic-autoops"
      path        = "/webhooks/elastic/autoops"
      topic       = "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"
      dlqTopic    = "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"
      keyFields   = ["resourceId", "deployment-id"]
      idempotency = "elastic-autoops"
    }
  ]
}
```

## IAM Roles

| Role | Used by | Minimum permissions |
|---|---|---|
| **Execution role** | ECS agent (image pull, log push, secret fetch) | `AmazonECSTaskExecutionRolePolicy` + `secretsmanager:GetSecretValue` on Confluent secret ARNs (if applicable) |
| **Task role** (`taskRoleArn`) | Application code inside the container | MSK IAM auth: `kafka:GetBootstrapBrokers`, `kafka:DescribeCluster` on the cluster ARN; `kafka-cluster:Connect`, `kafka-cluster:DescribeTopic` on the cluster; `kafka-cluster:WriteData`, `kafka-cluster:WriteDataIdempotently` on each `T_PRIVATE_SOURCE_*` topic ARN |
| **Infrastructure role** (`volumeConfigurations[].managedEBSVolume.roleArn`) | ECS service control plane (EBS lifecycle) | `ec2:CreateVolume`, `ec2:AttachVolume`, `ec2:DetachVolume`, `ec2:DescribeVolumes`, `ec2:DeleteVolume`, `ec2:CreateTags` |

## Verification

After registering the task definition and creating the service:

```bash
aws ecs wait services-stable \
  --cluster eventgate-cluster \
  --services eventgate-gateway

aws ecs describe-services \
  --cluster eventgate-cluster \
  --services eventgate-gateway \
  --query 'services[].{name:serviceName,desired:desiredCount,running:runningCount,deployments:deployments[].rolloutState}'

# Healthcheck against ALB DNS
curl -fsS https://<alb-dns>/healthz
```

Expected: `runningCount == desiredCount`, `rolloutState == COMPLETED`, `/healthz` returns `200 {"ok":true,...}`. CloudWatch Logs at `/eventgate/gateway` should show `kafka provider selected` with the expected provider and `gateway listening` shortly after task start.

## See Also

- [aws-ecs.md](aws-ecs.md) — deployment topology and pipeline.
- [container-image.md](container-image.md) — image build, tiers, healthcheck command.
- [../architecture/outbox.md](../architecture/outbox.md) — why the EBS mount is necessary.
- [../architecture/kafka-provider-factory.md](../architecture/kafka-provider-factory.md) — provider env-var contracts.
- [../configuration/environment-variables.md](../configuration/environment-variables.md) — every variable referenced above.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-20 | Initial task definition example doc (SIO-807) |
