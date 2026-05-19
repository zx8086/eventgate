#!/usr/bin/env bash
# scripts/deploy/01-network.sh
# Creates VPC, 2 public + 2 private subnets, IGW, NAT GW.

SCRIPT_NAME="01-network"
source "$(dirname "$0")/lib.sh"

readonly VPC_CIDR="10.0.0.0/16"
readonly AZ_A="${AWS_REGION}a"
readonly AZ_B="${AWS_REGION}b"

existing="$(aws ec2 describe-vpcs --filters Name=tag:Name,Values=eventgate-vpc \
  --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)"

if [[ "$existing" != "None" && -n "$existing" ]]; then
  log "VPC already exists: $existing"
  vpc_id="$existing"
else
  vpc_id="$(aws ec2 create-vpc --cidr-block "$VPC_CIDR" \
    --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=eventgate-vpc}]" \
    --query 'Vpc.VpcId' --output text)"
  aws ec2 modify-vpc-attribute --vpc-id "$vpc_id" --enable-dns-hostnames
  aws ec2 modify-vpc-attribute --vpc-id "$vpc_id" --enable-dns-support
  log "created VPC $vpc_id"
fi
write_env VPC_ID "$vpc_id"

create_subnet() {
  local name="$1" cidr="$2" az="$3"
  local sid
  sid="$(aws ec2 describe-subnets --filters Name=tag:Name,Values="$name" \
    --query 'Subnets[0].SubnetId' --output text 2>/dev/null || echo None)"
  if [[ "$sid" == "None" || -z "$sid" ]]; then
    sid="$(aws ec2 create-subnet --vpc-id "$vpc_id" --cidr-block "$cidr" \
      --availability-zone "$az" \
      --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=$name}]" \
      --query 'Subnet.SubnetId' --output text)"
    log "created subnet $name=$sid"
  else
    log "subnet $name already exists: $sid"
  fi
  echo "$sid"
}

pub_a="$(create_subnet eventgate-public-a 10.0.0.0/24 "$AZ_A")"
pub_b="$(create_subnet eventgate-public-b 10.0.1.0/24 "$AZ_B")"
prv_a="$(create_subnet eventgate-private-a 10.0.10.0/24 "$AZ_A")"
prv_b="$(create_subnet eventgate-private-b 10.0.11.0/24 "$AZ_B")"

write_env PUBLIC_SUBNET_A "$pub_a"
write_env PUBLIC_SUBNET_B "$pub_b"
write_env PRIVATE_SUBNET_A "$prv_a"
write_env PRIVATE_SUBNET_B "$prv_b"

# Public subnets get auto-assign public IP for ALB nodes.
aws ec2 modify-subnet-attribute --subnet-id "$pub_a" --map-public-ip-on-launch
aws ec2 modify-subnet-attribute --subnet-id "$pub_b" --map-public-ip-on-launch

igw_id="$(aws ec2 describe-internet-gateways \
  --filters Name=attachment.vpc-id,Values="$vpc_id" \
  --query 'InternetGateways[0].InternetGatewayId' --output text 2>/dev/null || echo None)"
if [[ "$igw_id" == "None" || -z "$igw_id" ]]; then
  igw_id="$(aws ec2 create-internet-gateway \
    --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=eventgate-igw}]" \
    --query 'InternetGateway.InternetGatewayId' --output text)"
  aws ec2 attach-internet-gateway --vpc-id "$vpc_id" --internet-gateway-id "$igw_id"
  log "created and attached IGW $igw_id"
fi
write_env IGW_ID "$igw_id"

eip_id="$(read_env NAT_EIP_ID)"
if [[ -z "$eip_id" ]]; then
  eip_id="$(aws ec2 allocate-address --domain vpc \
    --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=eventgate-nat-eip}]" \
    --query AllocationId --output text)"
  log "allocated NAT EIP $eip_id"
fi
write_env NAT_EIP_ID "$eip_id"

nat_id="$(aws ec2 describe-nat-gateways \
  --filter Name=tag:Name,Values=eventgate-nat Name=state,Values=available,pending \
  --query 'NatGateways[0].NatGatewayId' --output text 2>/dev/null || echo None)"
if [[ "$nat_id" == "None" || -z "$nat_id" ]]; then
  nat_id="$(aws ec2 create-nat-gateway --subnet-id "$pub_a" --allocation-id "$eip_id" \
    --tag-specifications "ResourceType=natgateway,Tags=[{Key=Name,Value=eventgate-nat}]" \
    --query 'NatGateway.NatGatewayId' --output text)"
  log "created NAT GW $nat_id; waiting for it to be available"
  aws ec2 wait nat-gateway-available --nat-gateway-ids "$nat_id"
fi
write_env NAT_GW_ID "$nat_id"

# Route tables. One public (default route to IGW), one private (default route to NAT).
ensure_rt() {
  local name="$1"
  local rid
  rid="$(aws ec2 describe-route-tables --filters Name=tag:Name,Values="$name" \
    --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null || echo None)"
  if [[ "$rid" == "None" || -z "$rid" ]]; then
    rid="$(aws ec2 create-route-table --vpc-id "$vpc_id" \
      --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=$name}]" \
      --query 'RouteTable.RouteTableId' --output text)"
  fi
  echo "$rid"
}

pub_rt="$(ensure_rt eventgate-public-rt)"
prv_rt="$(ensure_rt eventgate-private-rt)"
write_env PUBLIC_RT_ID "$pub_rt"
write_env PRIVATE_RT_ID "$prv_rt"

aws ec2 create-route --route-table-id "$pub_rt" --destination-cidr-block 0.0.0.0/0 \
  --gateway-id "$igw_id" 2>/dev/null || log "public default route exists"
aws ec2 create-route --route-table-id "$prv_rt" --destination-cidr-block 0.0.0.0/0 \
  --nat-gateway-id "$nat_id" 2>/dev/null || log "private default route exists"

associate_rt() {
  local rt="$1" subnet="$2"
  local existing
  existing="$(aws ec2 describe-route-tables --route-table-ids "$rt" \
    --query "RouteTables[0].Associations[?SubnetId=='$subnet'].RouteTableAssociationId" \
    --output text)"
  if [[ -z "$existing" ]]; then
    aws ec2 associate-route-table --route-table-id "$rt" --subnet-id "$subnet" >/dev/null
    log "associated $subnet with $rt"
  fi
}

associate_rt "$pub_rt" "$pub_a"
associate_rt "$pub_rt" "$pub_b"
associate_rt "$prv_rt" "$prv_a"
associate_rt "$prv_rt" "$prv_b"

log "network done"
