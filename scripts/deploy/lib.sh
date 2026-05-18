#!/usr/bin/env bash
# scripts/deploy/lib.sh
# Shared helpers for the eventgate AWS deploy scripts.
# Source this from every deploy script: `source "$(dirname "$0")/lib.sh"`.

set -euo pipefail

export AWS_REGION="${AWS_REGION:-eu-central-1}"
export AWS_PAGER=""

readonly ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.env.aws"

log() { printf '%s [%s] %s\n' "$(date -u +%H:%M:%S)" "${SCRIPT_NAME:-deploy}" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# Read a key from .env.aws (returns empty string if missing).
read_env() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    echo ""
    return 0
  fi
  grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true
}

# Write or update a key in .env.aws.
write_env() {
  local key="$1"
  local value="$2"
  mkdir -p "$(dirname "$ENV_FILE")"
  touch "$ENV_FILE"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    # Use a temp file to avoid sed -i portability issues between GNU and BSD.
    awk -v k="$key" -v v="$value" -F= '
      $1 == k { print k "=" v; next }
      { print }
    ' "$ENV_FILE" > "$ENV_FILE.tmp"
    mv "$ENV_FILE.tmp" "$ENV_FILE"
  else
    echo "${key}=${value}" >> "$ENV_FILE"
  fi
  log "wrote $key=$value to .env.aws"
}

require_env() {
  local key="$1"
  local value
  value="$(read_env "$key")"
  if [[ -z "$value" ]]; then
    die "$key is not set in $ENV_FILE — run the earlier phase first"
  fi
  echo "$value"
}

aws_account_id() {
  aws sts get-caller-identity --query Account --output text
}
