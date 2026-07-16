#!/usr/bin/env bash
# Retry `terraform apply` until it succeeds or MAX_ATTEMPTS is hit.
#
# OCI's A1.Flex Always Free capacity is contested; "Out of host capacity"
# on the compute instance is common and not a config problem -- retrying
# is the standard workaround, not a bug in this Terraform.
#
# This is meant for YOU to run, knowingly, against your own real OCI
# account (with -auto-approve, since nothing will be there to approve
# interactively at 3am) -- not something to invoke unattended without
# understanding it will keep hitting a real billing account for hours.
set -euo pipefail

cd "$(dirname "$0")/.."

MAX_ATTEMPTS="${MAX_ATTEMPTS:-48}" # 48 * 10min = 8 hours
SLEEP_SECONDS="${SLEEP_SECONDS:-600}"

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  echo "[$(date -u +%FT%TZ)] attempt $attempt/$MAX_ATTEMPTS"
  if terraform apply -auto-approve; then
    echo "[$(date -u +%FT%TZ)] apply succeeded"
    exit 0
  fi
  echo "[$(date -u +%FT%TZ)] apply failed (likely Out of host capacity), sleeping ${SLEEP_SECONDS}s"
  sleep "$SLEEP_SECONDS"
done

echo "[$(date -u +%FT%TZ)] gave up after $MAX_ATTEMPTS attempts"
exit 1
