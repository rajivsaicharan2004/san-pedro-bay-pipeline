"""Minimal Stage 5 dashboard skeleton: ships at anchor right now.

Runs on Streamlit Community Cloud, which is not this pipeline's OCI
instance and can't use instance principal auth or read the local DuckDB
file -- it fetches a small JSON snapshot from a Pre-Authenticated Request
(a public, read-only URL for exactly one object) that
orchestration/spb_orchestration/dashboard_export.py keeps overwriting.
See infra/oci/scripts/create_dashboard_par.sh for how that URL is created.
"""
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="San Pedro Bay -- Ships at Anchor", page_icon="⚓")

PAR_URL = st.secrets.get("PAR_URL", "")


@st.cache_data(ttl=60)
def fetch_snapshot(url: str) -> dict:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


st.title("⚓ Ships at Anchor -- San Pedro Bay")

if not PAR_URL:
    st.error(
        "No PAR_URL configured. Add it to this app's Secrets in Streamlit "
        "Community Cloud (Settings -> Secrets): `PAR_URL = \"https://...\"` "
        "-- see infra/oci/scripts/create_dashboard_par.sh."
    )
    st.stop()

try:
    snapshot = fetch_snapshot(PAR_URL)
except requests.RequestException as e:
    st.error(f"Couldn't fetch the snapshot: {e}")
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
