#!/usr/bin/env bash
# scripts/deploy/07-alb.sh
# Creates the public ALB, target group, and HTTP :80 listener.

SCRIPT_NAME="07-alb"
source "$(dirname "$0")/lib.sh"

vpc_id="$(require_env VPC_ID)"
pub_a="$(require_env PUBLIC_SUBNET_A)"
pub_b="$(require_env PUBLIC_SUBNET_B)"
sg_alb="$(require_env SG_ALB)"

# Target group first (listener forwards to it).
tg_arn="$(aws elbv2 describe-target-groups --names eventgate-gateway-tg \
  --query 'TargetGroups[0].TargetGroupArn' --output text 2>/dev/null || echo None)"
if [[ "$tg_arn" == "None" || -z "$tg_arn" ]]; then
  tg_arn="$(aws elbv2 create-target-group \
    --name eventgate-gateway-tg \
    --protocol HTTP --port 3000 \
    --vpc-id "$vpc_id" \
    --target-type ip \
    --health-check-protocol HTTP \
    --health-check-path /healthz \
    --health-check-interval-seconds 30 \
    --healthy-threshold-count 2 \
    --unhealthy-threshold-count 3 \
    --query 'TargetGroups[0].TargetGroupArn' --output text)"
  log "created target group: $tg_arn"
else
  log "target group already exists: $tg_arn"
fi
write_env GATEWAY_TG_ARN "$tg_arn"

# ALB.
alb_arn="$(aws elbv2 describe-load-balancers --names eventgate-alb \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text 2>/dev/null || echo None)"
if [[ "$alb_arn" == "None" || -z "$alb_arn" ]]; then
  alb_arn="$(aws elbv2 create-load-balancer \
    --name eventgate-alb \
    --type application \
    --scheme internet-facing \
    --subnets "$pub_a" "$pub_b" \
    --security-groups "$sg_alb" \
    --query 'LoadBalancers[0].LoadBalancerArn' --output text)"
  log "created ALB: $alb_arn"
else
  log "ALB already exists: $alb_arn"
fi
write_env ALB_ARN "$alb_arn"

alb_dns="$(aws elbv2 describe-load-balancers --load-balancer-arns "$alb_arn" \
  --query 'LoadBalancers[0].DNSName' --output text)"
write_env ALB_DNS "$alb_dns"

# Listener.
listener_arn="$(aws elbv2 describe-listeners --load-balancer-arn "$alb_arn" \
  --query "Listeners[?Port==\`80\`].ListenerArn | [0]" --output text 2>/dev/null || echo None)"
if [[ "$listener_arn" == "None" || -z "$listener_arn" ]]; then
  listener_arn="$(aws elbv2 create-listener \
    --load-balancer-arn "$alb_arn" \
    --protocol HTTP --port 80 \
    --default-actions "Type=forward,TargetGroupArn=$tg_arn" \
    --query 'Listeners[0].ListenerArn' --output text)"
  log "created listener: $listener_arn"
else
  log "listener already exists: $listener_arn"
fi
write_env LISTENER_ARN "$listener_arn"

log "ALB DNS: $alb_dns"
