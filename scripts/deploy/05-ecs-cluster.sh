#!/usr/bin/env bash
SCRIPT_NAME="05-ecs-cluster"
source "$(dirname "$0")/lib.sh"

existing="$(aws ecs describe-clusters --clusters eventgate \
  --query 'clusters[?status==`ACTIVE`].clusterArn | [0]' --output text 2>/dev/null || echo None)"

if [[ "$existing" != "None" && -n "$existing" ]]; then
  log "ECS cluster already exists: $existing"
  arn="$existing"
else
  arn="$(aws ecs create-cluster --cluster-name eventgate \
    --capacity-providers FARGATE --query 'cluster.clusterArn' --output text)"
  log "created ECS cluster: $arn"
fi
write_env ECS_CLUSTER_ARN "$arn"
