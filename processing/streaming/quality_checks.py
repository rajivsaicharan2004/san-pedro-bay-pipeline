"""Data quality checks for AIS positions, enforced in the stream.

A Pandera DataFrameSchema, applied to plain pandas DataFrames -- this is
plugged into the Spark pipeline via mapInPandas (a per-partition
transform with no windowing/aggregation, so unlike the row_number()-based
zone tie-break in spatial_join.py, it's fully streaming-safe), but every
check here is also directly unit-testable with a bare pandas DataFrame
(tests/test_quality_checks.py), same philosophy as vessel_state_logic.py.

Violations are never silently dropped: validate_positions() tags every
row with quality_ok/quality_failures and leaves it to the caller
(vessel_state_job.py) to route failing rows to positions_quarantine
instead of positions_silver.
"""
from datetime import timedelta

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

# Same box the producer subscribes AISStream with
# (ingestion/ais_producer/producer.py) -- a position outside this is
# either a corrupted lat/lon or a bug in that filter, not real data we
# asked for.
LAT_MIN, LAT_MAX = 33.55, 33.78
LON_MIN, LON_MAX = -118.30, -118.05

# Knots. Also excludes AIS's spec-defined 102.3 "speed not available"
# sentinel, which would otherwise read as a plausible-looking float.
SOG_MIN, SOG_MAX = 0.0, 40.0

# Max allowed gap between the AIS-reported event time and when our
# producer received it (both fixed at production time). Deliberately
# compared against ingested_at rather than wall-clock "now" so this check
# gives the same answer whether run against a live stream or replayed
# against a historical backlog.
MAX_SKEW = timedelta(minutes=5)

POSITION_SCHEMA = DataFrameSchema(
    {
        "mmsi": Column(str, Check.str_matches(r"^\d{9}$"), nullable=False),
        "lat": Column(float, Check.in_range(LAT_MIN, LAT_MAX), nullable=False),
        "lon": Column(float, Check.in_range(LON_MIN, LON_MAX), nullable=False),
        "sog": Column(float, Check.in_range(SOG_MIN, SOG_MAX), nullable=False),
    },
    strict=False,  # extra columns (event_time, zone_id, ...) pass through untouched
    coerce=False,
)


def _skew_failures(df: pd.DataFrame) -> pd.Series:
    skew = (df["ingested_at"] - df["event_time"]).abs()
    return skew > MAX_SKEW


def validate_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Adds quality_ok (bool) and quality_failures (comma-joined reason
    string, or None) columns. Never drops or reorders rows."""
    df = df.reset_index(drop=True)

    try:
        POSITION_SCHEMA.validate(df, lazy=True)
        failure_cases = pd.DataFrame(columns=["index", "column", "check"])
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases

    reasons_by_row = {}
    for _, fc in failure_cases.iterrows():
        idx = int(fc["index"])
        reasons_by_row.setdefault(idx, []).append(f"{fc['column']}:{fc['check']}")

    for idx in df.index[_skew_failures(df)]:
        reasons_by_row.setdefault(idx, []).append("timestamp_skew")

    result = df.copy()
    result["quality_failures"] = [
        ",".join(reasons_by_row[i]) if i in reasons_by_row else None for i in df.index
    ]
    result["quality_ok"] = result["quality_failures"].isna()
    return result
