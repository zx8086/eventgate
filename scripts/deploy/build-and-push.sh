#!/usr/bin/env bash
# scripts/deploy/build-and-push.sh
# Build the eventgate image and push to ECR. Tags with git short SHA + 'latest'.

SCRIPT_NAME="build-and-push"
source "$(dirname "$0")/lib.sh"

repo_uri="$(require_env ECR_REPO_URI)"
account="$(aws_account_id)"
sha="$(git rev-parse --short HEAD)"

# repo_uri is e.g. 123456789012.dkr.ecr.eu-central-1.amazonaws.com/eventgate
registry="${repo_uri%/eventgate}"

log "logging into ECR ($registry)"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$registry"

log "building eventgate:$sha"
docker build --platform linux/amd64 -t "eventgate:$sha" \
  "$(git rev-parse --show-toplevel)"

docker tag "eventgate:$sha" "$repo_uri:$sha"
docker tag "eventgate:$sha" "$repo_uri:latest"

log "pushing $repo_uri:$sha and :latest"
docker push "$repo_uri:$sha"
docker push "$repo_uri:latest"

write_env IMAGE_TAG "$sha"
write_env IMAGE_URI "$repo_uri:$sha"
log "image pushed: $repo_uri:$sha"
