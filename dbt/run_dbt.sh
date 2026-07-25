#!/usr/bin/env bash
# Exit criterion for Stage 3: `dbt build` green against real cloud data.
# Forces one fresh sync immediately before building so a manual run never
# tests against a stale mirror -- the systemd timer keeps it warm
# in between runs, this just removes any doubt for an on-demand check.
set -euo pipefail

cd "$(dirname "$0")/.."
./infra/oci/scripts/sync_lakehouse.sh

cd dbt
./.venv/bin/dbt build --profiles-dir .
