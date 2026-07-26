"""Minimal Stage 5 dashboard skeleton: ships at anchor right now.

Two ways to get the snapshot, both produced by
orchestration/spb_orchestration/dashboard_export.py:

- SNAPSHOT_FILE secret set: this app and the pipeline are running on the
  same machine (a local persistent deployment) -- just read the file
  directly, no network hop needed.
- PAR_URL secret set instead: this app is on Streamlit Community Cloud,
  which is not the pipeline's box and can't read its local DuckDB file or
  use instance principal auth -- fetches a small JSON snapshot from a
  Pre-Authenticated Request (a public, read-only URL for exactly one
  object). See infra/oci/scripts/create_dashboard_par.sh.
"""
import json
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="San Pedro Bay -- Ships at Anchor", page_icon="⚓")

SNAPSHOT_FILE = st.secrets.get("SNAPSHOT_FILE", "")
PAR_URL = st.secrets.get("PAR_URL", "")


@st.cache_data(ttl=60)
def fetch_snapshot_from_url(url: str) -> dict:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@st.cache_data(ttl=15)
def read_snapshot_from_file(path: str) -> dict:
    return json.loads(Path(path).read_text())


st.title("⚓ Ships at Anchor -- San Pedro Bay")

if not SNAPSHOT_FILE and not PAR_URL:
    st.error(
        "No data source configured. Add either `SNAPSHOT_FILE = \"/path/to/snapshot.json\"` "
        "(local deployment) or `PAR_URL = \"https://...\"` (Streamlit Community Cloud) "
        "to this app's Secrets."
    )
    st.stop()

try:
    snapshot = (
        read_snapshot_from_file(SNAPSHOT_FILE)
        if SNAPSHOT_FILE
        else fetch_snapshot_from_url(PAR_URL)
    )
except (OSError, requests.RequestException) as e:
    st.error(f"Couldn't read the snapshot: {e}")
    st.stop()

ships = snapshot.get("ships_at_anchor", [])

st.metric("Ships currently at anchor", len(ships))
st.caption(f"Snapshot generated at {snapshot.get('generated_at', 'unknown')} UTC")

if ships:
    df = pd.DataFrame(ships)
    df = df.rename(
        columns={
            "mmsi": "MMSI",
            "ship_name": "Ship",
            "anchorage_name": "Anchorage",
            "arrival_time": "Arrived",
            "minutes_at_anchor": "Minutes at anchor",
        }
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No ships currently at anchor.")
