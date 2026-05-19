#!/usr/bin/env bash
SCRIPT_NAME="04-ecr"
source "$(dirname "$0")/lib.sh"

existing="$(aws ecr describe-repositories --repository-names eventgate \
  --query 'repositories[0].repositoryUri' --output text 2>/dev/null || echo None)"

if [[ "$existing" != "None" && -n "$existing" ]]; then
  log "ECR repo already exists: $existing"
  uri="$existing"
else
  uri="$(aws ecr create-repository --repository-name eventgate \
    --image-scanning-configuration scanOnPush=true \
    --query 'repository.repositoryUri' --output text)"
  log "created ECR repo: $uri"
fi
write_env ECR_REPO_URI "$uri"
