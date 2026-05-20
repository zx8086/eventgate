#!/usr/bin/env bash
# scripts/deploy/teardown.sh
# Deletes everything created by the deploy scripts, in reverse order.
# IMPORTANT: this WILL delete data. Use only in lab/dev.

SCRIPT_NAME="teardown"
source "$(dirname "$0")/lib.sh"

read -r -p "This will delete the entire eventgate AWS stack in $AWS_REGION. Type 'destroy' to confirm: " confirm
if [[ "$confirm" != "destroy" ]]; then
  die "aborted"
fi

cluster_arn="$(read_env ECS_CLUSTER_ARN)"

if [[ -n "$cluster_arn" ]]; then
  for svc in eventgate-gateway; do
    if aws ecs describe-services --cluster "$cluster_arn" --services "$svc" \
       --query "services[?status=='ACTIVE']" --output text | grep -q .; then
      log "scaling $svc to 0 and deleting"
      aws ecs update-service --cluster "$cluster_arn" --service "$svc" --desired-count 0 >/dev/null || true
      aws ecs delete-service --cluster "$cluster_arn" --service "$svc" --force >/dev/null || true
    fi
  done
  aws ecs delete-cluster --cluster "$cluster_arn" >/dev/null || true
  log "deleted ECS cluster"
fi

listener_arn="$(read_env LISTENER_ARN)"
[[ -n "$listener_arn" ]] && aws elbv2 delete-listener --listener-arn "$listener_arn" 2>/dev/null || true

alb_arn="$(read_env ALB_ARN)"
[[ -n "$alb_arn" ]] && aws elbv2 delete-load-balancer --load-balancer-arn "$alb_arn" 2>/dev/null || true

tg_arn="$(read_env GATEWAY_TG_ARN)"
[[ -n "$tg_arn" ]] && aws elbv2 delete-target-group --target-group-arn "$tg_arn" 2>/dev/null || true

msk_arn="$(read_env MSK_CLUSTER_ARN)"
if [[ -n "$msk_arn" ]]; then
  log "deleting MSK cluster (this takes a few minutes)"
  aws kafka delete-cluster --cluster-arn "$msk_arn" || true
fi

for role in eventgate-gateway-task; do
  aws iam delete-role-policy --role-name "$role" --policy-name eventgate-msk-access 2>/dev/null || true
  aws iam delete-role --role-name "$role" 2>/dev/null || true
done
aws iam detach-role-policy --role-name eventgate-task-execution \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy 2>/dev/null || true
aws iam delete-role --role-name eventgate-task-execution 2>/dev/null || true

# Log groups
for lg in /eventgate/gateway; do
  aws logs delete-log-group --log-group-name "$lg" 2>/dev/null || true
done

# ECR (must delete images first if any)
aws ecr delete-repository --repository-name eventgate --force 2>/dev/null || true

# Network — VPC, NAT, EIP, IGW, subnets, route tables
nat_id="$(read_env NAT_GW_ID)"
if [[ -n "$nat_id" ]]; then
  aws ec2 delete-nat-gateway --nat-gateway-id "$nat_id" 2>/dev/null || true
  log "waiting for NAT gateway to delete"
  aws ec2 wait nat-gateway-deleted --nat-gateway-ids "$nat_id" 2>/dev/null || true
fi
eip_id="$(read_env NAT_EIP_ID)"
[[ -n "$eip_id" ]] && aws ec2 release-address --allocation-id "$eip_id" 2>/dev/null || true

for sg in SG_ALB SG_GATEWAY SG_MSK; do
  sgid="$(read_env "$sg")"
  [[ -n "$sgid" ]] && aws ec2 delete-security-group --group-id "$sgid" 2>/dev/null || true
done

vpc_id="$(read_env VPC_ID)"
igw_id="$(read_env IGW_ID)"
if [[ -n "$igw_id" && -n "$vpc_id" ]]; then
  aws ec2 detach-internet-gateway --internet-gateway-id "$igw_id" --vpc-id "$vpc_id" 2>/dev/null || true
  aws ec2 delete-internet-gateway --internet-gateway-id "$igw_id" 2>/dev/null || true
fi
for s in PUBLIC_SUBNET_A PUBLIC_SUBNET_B PRIVATE_SUBNET_A PRIVATE_SUBNET_B; do
  sid="$(read_env "$s")"
  [[ -n "$sid" ]] && aws ec2 delete-subnet --subnet-id "$sid" 2>/dev/null || true
done
for r in PUBLIC_RT_ID PRIVATE_RT_ID; do
  rid="$(read_env "$r")"
  [[ -n "$rid" ]] && aws ec2 delete-route-table --route-table-id "$rid" 2>/dev/null || true
done
[[ -n "$vpc_id" ]] && aws ec2 delete-vpc --vpc-id "$vpc_id" 2>/dev/null || true

log "teardown complete. .env.aws preserved for audit; delete manually if desired."
