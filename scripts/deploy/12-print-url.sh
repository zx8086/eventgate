#!/usr/bin/env bash
SCRIPT_NAME="12-print-url"
source "$(dirname "$0")/lib.sh"

alb_dns="$(require_env ALB_DNS)"

echo
echo "=================================================="
echo "Webhook URL for Elastic AutoOps:"
echo "  http://${alb_dns}/webhooks/elastic/autoops"
echo
echo "Health check:"
echo "  http://${alb_dns}/healthz"
echo "=================================================="
