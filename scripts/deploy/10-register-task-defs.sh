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
