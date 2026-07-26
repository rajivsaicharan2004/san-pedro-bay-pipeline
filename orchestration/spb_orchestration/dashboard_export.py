"""Publishes a tiny "ships at anchor right now" snapshot for the Stage 5
Streamlit dashboard.

Two delivery modes, chosen by whether SNAPSHOT_OUTPUT_FILE is set:

- Cloud (default, SNAPSHOT_OUTPUT_FILE unset): Streamlit runs on
  Community Cloud, outside this box entirely, so it can't read the local
  DuckDB file or use instance principal auth the way everything else in
  this pipeline does. The snapshot is uploaded to the lakehouse bucket at
  a fixed object name, readable via a Pre-Authenticated Request (a plain
  public HTTPS URL for exactly that one object) --
  infra/oci/scripts/create_dashboard_par.sh creates that PAR once; this
  asset just keeps overwriting the object underneath the same URL.

- Local (SNAPSHOT_OUTPUT_FILE set): Streamlit also runs on this same
  machine, so there's no cloud hop needed at all -- this just writes the
  JSON straight to a file dashboard/streamlit_app.py reads directly.

Depends on fct_anchorage_dwell (a dbt asset) so it materializes as part
of the same sensor/schedule-driven pipeline from Stage 4, not a separate
timer with its own drift.
"""
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dagster import AssetExecutionContext, asset

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/home/ubuntu/spb_gold.duckdb")
OCI_CLI_BIN = os.getenv("OCI_CLI_BIN", "/home/ubuntu/.oci-cli-venv/bin/oci")
NAMESPACE = "ax5rxkwswz5n"
BUCKET = "san-pedro-bay-lakehouse"
OBJECT_NAME = "dashboard/ships_at_anchor_now.json"
SNAPSHOT_OUTPUT_FILE = os.getenv("SNAPSHOT_OUTPUT_FILE", "")

QUERY = """
select
    a.mmsi,
    v.ship_name,
    a.anchorage_name,
    a.arrival_time,
    date_diff('minute', a.arrival_time, current_timestamp) as minutes_at_anchor
from fct_anchorage_dwell a
left join dim_vessels v on a.mmsi = v.mmsi
where a.is_ongoing
order by a.arrival_time
"""


@asset(deps=["fct_anchorage_dwell", "dim_vessels"])
def ships_at_anchor_now_export(context: AssetExecutionContext) -> None:
    # Read-only: this box's Spark jobs and dbt are the only writers of
    # spb_gold.duckdb, and DuckDB only allows one writer connection at a
    # time -- opening this one read-only means it can never contend with
    # (or block on) whichever of those is mid-write.
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        rows = conn.sql(QUERY).fetchall()
        columns = [d[0] for d in conn.sql(QUERY).description]
    finally:
        conn.close()

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ships_at_anchor": [dict(zip(columns, row)) for row in rows],
    }
    context.log.info(f"{len(rows)} ship(s) at anchor")

    if SNAPSHOT_OUTPUT_FILE:
        out_path = Path(SNAPSHOT_OUTPUT_FILE)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(snapshot, default=str))
        tmp_path.replace(out_path)  # atomic on the same filesystem
        return

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(snapshot, f, default=str)
        tmp_path = f.name

    try:
        subprocess.run(
            [
                OCI_CLI_BIN, "os", "object", "put",
                "--namespace", NAMESPACE,
                "--bucket-name", BUCKET,
                "--object-name", OBJECT_NAME,
                "--file", tmp_path,
                "--content-type", "application/json",
                "--force",
                "--auth", "instance_principal",
            ],
            check=True,
        )
    finally:
        os.remove(tmp_path)
