#!/usr/bin/env bash
# scripts/deploy/02-security-groups.sh
# Creates sg-alb, sg-gateway, sg-msk and wires rules.

SCRIPT_NAME="02-security-groups"
source "$(dirname "$0")/lib.sh"

vpc_id="$(require_env VPC_ID)"

ensure_sg() {
  local name="$1" desc="$2"
  local sgid
  sgid="$(aws ec2 describe-security-groups \
    --filters Name=vpc-id,Values="$vpc_id" Name=group-name,Values="$name" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo None)"
  if [[ "$sgid" == "None" || -z "$sgid" ]]; then
    sgid="$(aws ec2 create-security-group --vpc-id "$vpc_id" --group-name "$name" \
      --description "$desc" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$name}]" \
      --query GroupId --output text)"
    log "created SG $name=$sgid"
  else
    log "SG $name already exists: $sgid"
  fi
  echo "$sgid"
}

sg_alb="$(ensure_sg eventgate-alb 'eventgate ALB ingress :80')"
sg_gw="$(ensure_sg eventgate-gateway 'eventgate gateway tasks')"
sg_msk="$(ensure_sg eventgate-msk 'eventgate MSK Serverless')"

write_env SG_ALB "$sg_alb"
write_env SG_GATEWAY "$sg_gw"
write_env SG_MSK "$sg_msk"

ingress_cidr() {
  local sg="$1" port="$2" cidr="$3"
  aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --protocol tcp --port "$port" --cidr "$cidr" 2>/dev/null \
    || log "ingress $cidr:$port on $sg already exists"
}

ingress_sg() {
  local sg="$1" port="$2" source_sg="$3"
  aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --ip-permissions "IpProtocol=tcp,FromPort=$port,ToPort=$port,UserIdGroupPairs=[{GroupId=$source_sg}]" \
    2>/dev/null || log "ingress sg=$source_sg:$port on $sg already exists"
}

ingress_cidr "$sg_alb" 80 0.0.0.0/0
ingress_sg   "$sg_gw"  3000 "$sg_alb"
ingress_sg   "$sg_msk" 9098 "$sg_gw"

log "security groups done"
