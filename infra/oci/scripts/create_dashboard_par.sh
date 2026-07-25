#!/usr/bin/env bash
# One-time setup, not part of the recurring pipeline: creates a
# Pre-Authenticated Request granting anonymous, read-only access to
# exactly one object (dashboard/ships_at_anchor_now.json) -- nothing else
# in the bucket. This is what lets a Streamlit Community Cloud app (which
# runs outside this box and can't use instance principal auth) fetch the
# dashboard snapshot with a plain HTTP GET and no credentials of any kind.
#
# The object doesn't need to exist yet -- ObjectRead PARs are valid for a
# name, checked at fetch time, not at creation time. Re-running this after
# the PAR already exists will just create a second one; check
# `oci os preauth-request list` first if unsure.
set -euo pipefail

NAMESPACE="ax5rxkwswz5n"
BUCKET="san-pedro-bay-lakehouse"
OBJECT_NAME="dashboard/ships_at_anchor_now.json"
PAR_NAME="spb-dashboard-ships-at-anchor"
TIME_EXPIRES="${TIME_EXPIRES:-2028-07-25T00:00:00Z}"
OCI_BIN="${OCI_BIN:-$HOME/.oci-cli-venv/bin/oci}"

result=$("$OCI_BIN" os preauth-request create \
  --namespace "$NAMESPACE" \
  --bucket-name "$BUCKET" \
  --name "$PAR_NAME" \
  --access-type ObjectRead \
  --object-name "$OBJECT_NAME" \
  --time-expires "$TIME_EXPIRES" \
  --auth instance_principal)

echo "$result"
echo
echo "Public URL (paste into dashboard/streamlit_app.py or Streamlit Cloud secrets):"
echo "$result" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['full-path'])"
echo
echo "Expires: $TIME_EXPIRES -- recreate before then (this script is idempotent-ish;"
echo "delete the old PAR first with 'oci os preauth-request delete' or you'll end up with two)."
