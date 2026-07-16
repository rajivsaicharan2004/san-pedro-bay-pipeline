from datetime import datetime, timedelta

from vessel_state_logic import (
    STATE_AT_ANCHOR,
    STATE_MOORED,
    STATE_UNDERWAY,
    STATE_UNKNOWN,
    VesselState,
    process_observation,
    process_timeout,
)

T0 = datetime(2026, 7, 15, 12, 0, 0)


def obs_at(minutes, zone_type, sog, nav_status_code=None, zone_id="cfr-anchorage-b"):
    return {
        "time": T0 + timedelta(minutes=minutes),
        "lat": 33.73,
        "lon": -118.22,
        "sog": sog,
        "zone_type": zone_type,
        "zone_id": zone_id if zone_type else None,
        "nav_status_code": nav_status_code,
    }


def test_first_observation_adopts_state_immediately_without_debounce():
    state = VesselState(mmsi="123")
    events = process_observation(state, obs_at(0, "anchorage", 0.2))

    assert state.current_state == STATE_AT_ANCHOR
    assert len(events) == 1
    assert events[0]["from_state"] == STATE_UNKNOWN
    assert events[0]["to_state"] == STATE_AT_ANCHOR
    assert events[0]["zone_id"] == "cfr-anchorage-b"


def test_sog_jitter_around_threshold_does_not_flap_state():
    """Anchored vessel whose SOG jitters 0.4/0.6 kn -- each excursion above
    the threshold is well under the debounce window, so it must never
    commit a transition to UNDERWAY."""
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "anchorage", 0.2))
    assert state.current_state == STATE_AT_ANCHOR

    jitter_events = []
    # Jitter for 20 minutes, each excursion above threshold lasting ~1 min,
    # well short of the 5-minute debounce.
    for minute in range(1, 21):
        sog = 0.6 if minute % 2 == 0 else 0.2
        jitter_events += process_observation(state, obs_at(minute, "anchorage", sog))

    assert state.current_state == STATE_AT_ANCHOR
    assert jitter_events == []


def test_sustained_departure_commits_after_debounce_window():
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "anchorage", 0.2))

    # Sustained high SOG, outside any zone, held continuously. Candidate
    # starts at minute 1, so commit requires t - candidate_since >= 5, i.e.
    # minute 6 -- minute 5 is one tick short and must not commit yet.
    events = []
    for minute in [1, 2, 3, 4, 5]:
        events += process_observation(state, obs_at(minute, None, 8.0))
    assert state.current_state == STATE_AT_ANCHOR, "must not flip before debounce elapses"
    assert events == []

    events += process_observation(state, obs_at(6, None, 8.0))
    assert state.current_state == STATE_UNDERWAY
    assert len(events) == 1
    assert events[0]["from_state"] == STATE_AT_ANCHOR
    assert events[0]["to_state"] == STATE_UNDERWAY


def test_moored_at_berth_vs_anchored_are_distinct_states():
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "berth", 0.1, zone_id="osm-way-1"))
    assert state.current_state == STATE_MOORED

    for minute in range(1, 7):
        process_observation(state, obs_at(minute, "anchorage", 0.1, zone_id="cfr-anchorage-b"))
    assert state.current_state == STATE_AT_ANCHOR


def test_discrepancy_flagged_after_sustained_disagreement():
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "anchorage", 0.1, nav_status_code=1))  # agrees: at anchor
    assert state.discrepancy_flagged is False

    # discrepancy_since starts at minute 1, so flagging requires
    # t - discrepancy_since >= 15, i.e. minute 16.
    events = []
    for minute in range(1, 16):
        events += process_observation(state, obs_at(minute, "anchorage", 0.1, nav_status_code=0))  # reports underway
    assert state.discrepancy_flagged is False, "must not flag before 15 minutes elapse"
    assert events == []

    events += process_observation(state, obs_at(16, "anchorage", 0.1, nav_status_code=0))
    assert state.discrepancy_flagged is True
    assert len(events) == 1
    assert events[0]["event_type"] == "discrepancy_flagged"
    assert events[0]["derived_state"] == STATE_AT_ANCHOR
    assert events[0]["reported_status"] == STATE_UNDERWAY

    # Must not re-flag every subsequent observation.
    more_events = process_observation(state, obs_at(17, "anchorage", 0.1, nav_status_code=0))
    assert more_events == []


def test_discrepancy_cleared_when_reports_reconcile():
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "anchorage", 0.1, nav_status_code=1))
    for minute in range(1, 17):
        process_observation(state, obs_at(minute, "anchorage", 0.1, nav_status_code=0))
    assert state.discrepancy_flagged is True

    events = process_observation(state, obs_at(17, "anchorage", 0.1, nav_status_code=1))
    assert state.discrepancy_flagged is False
    assert len(events) == 1
    assert events[0]["event_type"] == "discrepancy_cleared"


def test_ambiguous_nav_status_codes_never_flagged():
    """Code 2 (not under command) etc. bucket to OTHER and are excluded --
    too ambiguous to compare against our 3-state model."""
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "anchorage", 0.1, nav_status_code=1))

    events = []
    for minute in range(1, 30):
        events += process_observation(state, obs_at(minute, "anchorage", 0.1, nav_status_code=2))
    assert state.discrepancy_flagged is False
    assert events == []


def test_process_timeout_emits_expired_event_for_tracked_vessel():
    state = VesselState(mmsi="123")
    process_observation(state, obs_at(0, "anchorage", 0.1))

    events = process_timeout(state)
    assert len(events) == 1
    assert events[0]["from_state"] == STATE_AT_ANCHOR
    assert events[0]["to_state"] == "EXPIRED"


def test_process_timeout_on_never_observed_vessel_emits_nothing():
    state = VesselState(mmsi="123")
    assert process_timeout(state) == []
