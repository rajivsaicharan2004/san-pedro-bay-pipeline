from datetime import datetime, timedelta

import pandas as pd

from quality_checks import validate_positions

T0 = datetime(2026, 7, 15, 12, 0, 0)


def make_row(**overrides):
    row = {
        "mmsi": "367625810",
        "lat": 33.73,
        "lon": -118.22,
        "sog": 5.2,
        "event_time": T0,
        "ingested_at": T0 + timedelta(seconds=2),
    }
    row.update(overrides)
    return row


def test_valid_row_passes():
    df = pd.DataFrame([make_row()])
    result = validate_positions(df)
    assert result.loc[0, "quality_ok"]
    assert result.loc[0, "quality_failures"] is None


def test_out_of_bounding_box_latitude_fails():
    df = pd.DataFrame([make_row(lat=40.0)])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "lat" in result.loc[0, "quality_failures"]


def test_out_of_bounding_box_longitude_fails():
    df = pd.DataFrame([make_row(lon=-100.0)])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "lon" in result.loc[0, "quality_failures"]


def test_negative_sog_fails():
    df = pd.DataFrame([make_row(sog=-1.0)])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "sog" in result.loc[0, "quality_failures"]


def test_ais_speed_not_available_sentinel_is_rejected():
    """102.3 kn is the AIS spec's 'speed not available' sentinel, not a
    real speed -- it must fail the SOG range check, not be treated as a
    genuine 102-knot vessel."""
    df = pd.DataFrame([make_row(sog=102.3)])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "sog" in result.loc[0, "quality_failures"]


def test_mmsi_not_nine_digits_fails():
    df = pd.DataFrame([make_row(mmsi="12345")])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "mmsi" in result.loc[0, "quality_failures"]


def test_mmsi_non_numeric_fails():
    df = pd.DataFrame([make_row(mmsi="36762581X")])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "mmsi" in result.loc[0, "quality_failures"]


def test_timestamp_skew_beyond_tolerance_fails():
    df = pd.DataFrame([make_row(ingested_at=T0 + timedelta(minutes=10))])
    result = validate_positions(df)
    assert not result.loc[0, "quality_ok"]
    assert "timestamp_skew" in result.loc[0, "quality_failures"]


def test_timestamp_skew_within_tolerance_passes():
    df = pd.DataFrame([make_row(ingested_at=T0 + timedelta(minutes=4))])
    result = validate_positions(df)
    assert result.loc[0, "quality_ok"]


def test_multiple_simultaneous_failures_all_recorded():
    df = pd.DataFrame([make_row(lat=40.0, sog=-1.0, mmsi="bad")])
    result = validate_positions(df)
    failures = result.loc[0, "quality_failures"]
    assert not result.loc[0, "quality_ok"]
    assert "lat" in failures
    assert "sog" in failures
    assert "mmsi" in failures


def test_mixed_batch_only_flags_the_bad_rows():
    df = pd.DataFrame([
        make_row(mmsi="111111111"),
        make_row(mmsi="222222222", lat=99.0),
        make_row(mmsi="333333333"),
    ])
    result = validate_positions(df)
    assert result.loc[0, "quality_ok"]
    assert not result.loc[1, "quality_ok"]
    assert result.loc[2, "quality_ok"]
    # row order/count must be preserved -- callers route by position
    assert list(result["mmsi"]) == ["111111111", "222222222", "333333333"]
