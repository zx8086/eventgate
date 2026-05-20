#!/usr/bin/env bash
SCRIPT_NAME="06-log-groups"
source "$(dirname "$0")/lib.sh"

ensure_log_group() {
  local name="$1"
  if aws logs describe-log-groups --log-group-name-prefix "$name" \
    --query "logGroups[?logGroupName=='$name'] | length(@)" --output text | grep -q '^0$'; then
    aws logs create-log-group --log-group-name "$name"
    aws logs put-retention-policy --log-group-name "$name" --retention-in-days 7
    log "created log group $name"
  else
    log "log group $name already exists"
  fi
}

ensure_log_group /eventgate/gateway
