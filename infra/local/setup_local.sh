#!/usr/bin/env bash
# Sets up this pipeline as a persistent local deployment (launchd
# services instead of a cloud instance's systemd units) -- the pivot
# taken when OCI's A1.Flex capacity lottery didn't land and paying for a
# non-free shape wasn't the direction chosen either.
#
# Assumes: docker compose (redpanda/minio/console) already running,
# openjdk@17 installed via brew, .venv already set up at repo root, and
# schemas already registered (schemas/register_schemas.py).
set -euo pipefail

REPO_ROOT="/Users/jo/projects/san-pedro-bay-pipeline"
LOCAL_STATE="/Users/jo/spb_local"

mkdir -p "$LOCAL_STATE/logs" "$LOCAL_STATE/dagster_home" "$LOCAL_STATE/lakehouse_marker" "$LOCAL_STATE/dashboard_data"

# dbt gets its own venv, same reasoning as the cloud deployment's --
# dbt-duckdb/deltalake have nothing to do with the Spark jobs' deps.
if [ ! -f "$REPO_ROOT/dbt/.venv/bin/dbt" ]; then
  python3.12 -m venv "$REPO_ROOT/dbt/.venv"
  "$REPO_ROOT/dbt/.venv/bin/pip" install --upgrade pip
  "$REPO_ROOT/dbt/.venv/bin/pip" install -r "$REPO_ROOT/dbt/requirements.txt"
fi

# Generates dbt/target/manifest.json for @dbt_assets. `dbt parse` doesn't
# open a warehouse connection, so target doesn't matter here.
"$REPO_ROOT/dbt/.venv/bin/dbt" parse --project-dir "$REPO_ROOT/dbt" --profiles-dir "$REPO_ROOT/dbt" --target dev

# Dagster gets its own venv too.
if [ ! -f "$REPO_ROOT/orchestration/.venv/bin/dagster-daemon" ]; then
  python3.12 -m venv "$REPO_ROOT/orchestration/.venv"
  "$REPO_ROOT/orchestration/.venv/bin/pip" install --upgrade pip
  "$REPO_ROOT/orchestration/.venv/bin/pip" install -r "$REPO_ROOT/orchestration/requirements.txt"
fi

touch "$LOCAL_STATE/dagster_home/dagster.yaml"

for plist in "$REPO_ROOT"/infra/local/launchd/*.plist; do
  name=$(basename "$plist")
  cp "$plist" "$HOME/Library/LaunchAgents/$name"
  launchctl unload "$HOME/Library/LaunchAgents/$name" 2>/dev/null || true
  launchctl load "$HOME/Library/LaunchAgents/$name"
done

echo "Done. Check status with: launchctl list | grep com.spb"
echo "Logs at: $LOCAL_STATE/logs/"
echo "Dagit: http://localhost:3000"
