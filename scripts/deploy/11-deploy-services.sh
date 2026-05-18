#!/usr/bin/env bash
# scripts/deploy/11-deploy-services.sh
# Create or update the gateway and writer ECS services.

SCRIPT_NAME="11-deploy-services"
source "$(dirname "$0")/lib.sh"

cluster_arn="$(require_env ECS_CLUSTER_ARN)"
prv_a="$(require_env PRIVATE_SUBNET_A)"
prv_b="$(require_env PRIVATE_SUBNET_B)"
sg_gw="$(require_env SG_GATEWAY)"
sg_wr="$(require_env SG_WRITER)"
tg_arn="$(require_env GATEWAY_TG_ARN)"
gw_td="$(require_env GATEWAY_TASK_DEF_ARN)"
wr_td="$(require_env WRITER_TASK_DEF_ARN)"

service_exists() {
  local svc="$1"
  local status
  status="$(aws ecs describe-services --cluster "$cluster_arn" --services "$svc" \
    --query 'services[0].status' --output text 2>/dev/null || echo NONE)"
  [[ "$status" == "ACTIVE" ]]
}

# Gateway service (attached to ALB target group).
if service_exists eventgate-gateway; then
  log "updating gateway service to new task def"
  aws ecs update-service \
    --cluster "$cluster_arn" \
    --service eventgate-gateway \
    --task-definition "$gw_td" \
    --force-new-deployment >/dev/null
else
  log "creating gateway service"
  aws ecs create-service \
    --cluster "$cluster_arn" \
    --service-name eventgate-gateway \
    --task-definition "$gw_td" \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$prv_a,$prv_b],securityGroups=[$sg_gw],assignPublicIp=DISABLED}" \
    --load-balancers "targetGroupArn=$tg_arn,containerName=eventgate-gateway,containerPort=3000" \
    --health-check-grace-period-seconds 60 >/dev/null
fi

# Writer service (no LB).
if service_exists eventgate-writer; then
  log "updating writer service to new task def"
  aws ecs update-service \
    --cluster "$cluster_arn" \
    --service eventgate-writer \
    --task-definition "$wr_td" \
    --force-new-deployment >/dev/null
else
  log "creating writer service"
  aws ecs create-service \
    --cluster "$cluster_arn" \
    --service-name eventgate-writer \
    --task-definition "$wr_td" \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$prv_a,$prv_b],securityGroups=[$sg_wr],assignPublicIp=DISABLED}" >/dev/null
fi

log "waiting for gateway and writer to become stable"
aws ecs wait services-stable --cluster "$cluster_arn" \
  --services eventgate-gateway eventgate-writer
log "both services stable"
