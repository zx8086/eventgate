#!/usr/bin/env bash
# scripts/deploy/03-msk.sh
# Creates an MSK Serverless cluster with IAM auth.

SCRIPT_NAME="03-msk"
source "$(dirname "$0")/lib.sh"

prv_a="$(require_env PRIVATE_SUBNET_A)"
prv_b="$(require_env PRIVATE_SUBNET_B)"
sg_msk="$(require_env SG_MSK)"

existing_arn="$(aws kafka list-clusters-v2 \
  --cluster-name-filter eventgate-msk \
  --query 'ClusterInfoList[0].ClusterArn' --output text 2>/dev/null || echo None)"

if [[ "$existing_arn" != "None" && -n "$existing_arn" ]]; then
  log "MSK cluster already exists: $existing_arn"
  cluster_arn="$existing_arn"
else
  cluster_arn="$(aws kafka create-cluster-v2 \
    --cluster-name eventgate-msk \
    --serverless "VpcConfigs=[{SubnetIds=[$prv_a,$prv_b],SecurityGroupIds=[$sg_msk]}],ClientAuthentication={Sasl={Iam={Enabled=true}}}" \
    --query ClusterArn --output text)"
  log "created MSK Serverless cluster: $cluster_arn"
fi

write_env MSK_CLUSTER_ARN "$cluster_arn"

log "waiting for MSK cluster to become ACTIVE (this can take ~10 min)..."
while true; do
  state="$(aws kafka describe-cluster-v2 --cluster-arn "$cluster_arn" \
    --query 'ClusterInfo.State' --output text)"
  log "MSK state: $state"
  case "$state" in
    ACTIVE) break ;;
    FAILED|DELETING|MAINTENANCE) die "MSK cluster reached unexpected state: $state" ;;
    *) sleep 30 ;;
  esac
done

bootstrap="$(aws kafka get-bootstrap-brokers --cluster-arn "$cluster_arn" \
  --query 'BootstrapBrokerStringSaslIam' --output text)"
write_env MSK_BOOTSTRAP "$bootstrap"
log "MSK bootstrap brokers: $bootstrap"
