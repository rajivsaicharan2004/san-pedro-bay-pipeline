#!/usr/bin/env bash
# Mirrors the lakehouse bucket to local disk so dbt-duckdb can read it.
# DuckDB's httpfs extension only speaks S3-style static keys (the same
# Customer Secret Key that's broken on this tenancy -- see
# processing/streaming/avro_kafka_reader.py's comment), so this uses the
# oci CLI's instance_principal auth instead -- the same proven-working
# native auth path Spark itself uses via oci-hdfs-connector.
set -euo pipefail

NAMESPACE="ax5rxkwswz5n"
BUCKET="san-pedro-bay-lakehouse"
DEST_DIR="${LAKEHOUSE_SYNC_DIR:-$HOME/lakehouse_sync}"
OCI_BIN="${OCI_BIN:-$HOME/.oci-cli-venv/bin/oci}"

mkdir -p "$DEST_DIR"
"$OCI_BIN" os object sync \
  --namespace "$NAMESPACE" \
  --bucket-name "$BUCKET" \
  --dest-dir "$DEST_DIR" \
  --auth instance_principal

# Dagster's sensor (orchestration/) polls this file's mtime rather than
# scanning every Delta file under $DEST_DIR to decide whether new data
# has landed -- one stat() call instead of walking the whole mirror.
touch "$DEST_DIR/.last_synced"
