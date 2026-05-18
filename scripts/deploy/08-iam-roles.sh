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
