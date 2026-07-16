"""Pure-Python vessel state machine: debounced state transitions + a
derived-vs-reported discrepancy flag. No Spark/pandas dependency on
purpose -- this is the one part of the stateful job worth unit testing in
isolation, since it's where the actual behavior (hysteresis, timing) lives.
The Spark applyInPandasWithState wrapper (vessel_state_job.py) is a thin
adapter around this.

State machine:
  UNDERWAY  -- default; SOG >= threshold, or not inside a zone
  AT_ANCHOR -- SOG < threshold, sustained, inside an anchorage zone
  MOORED    -- SOG < threshold, sustained, inside a berth zone

Debouncing: a candidate state must hold continuously for DEBOUNCE before
it's committed as the new current_state. This is what stops SOG jitter
around the 0.5 kn threshold (a vessel idling at 0.4/0.6 kn) from flapping
the derived state on every position report.

Discrepancy: reported_status (from AIS NavigationalStatus) is compared
against current_state (the debounced value -- treated as primary truth,
per design). Sustained disagreement for DISCREPANCY_THRESHOLD raises a
flag; agreement clears it. NavigationalStatus codes outside {0, 1, 5, 8}
bucket to OTHER and are not compared (too ambiguous to call a discrepancy).
"""
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

SOG_THRESHOLD_KN = 0.5

# "N consecutive observations (or M minutes)": M minutes is used here, not
# a fixed observation count, because AIS reporting interval varies a lot
# (as fast as 2s underway, as slow as ~3min at anchor per the class A
# default rates) -- a count-based debounce would mean a wildly different
# real-world duration depending on how the vessel happens to be reporting.
DEBOUNCE = timedelta(minutes=5)

DISCREPANCY_THRESHOLD = timedelta(minutes=15)

# How long a vessel can go without a new position before we consider it to
# have left the bounding box and expire its state.
TIMEOUT_AFTER = timedelta(minutes=30)

STATE_UNDERWAY = "UNDERWAY"
STATE_AT_ANCHOR = "AT_ANCHOR"
STATE_MOORED = "MOORED"
STATE_UNKNOWN = "UNKNOWN"

# AIS NavigationalStatus (Type 1/2/3 field): 0=under way using engine,
# 1=at anchor, 5=moored, 8=under way sailing. Everything else (restricted
# maneuverability, aground, fishing, reserved codes, 15=undefined, ...)
# doesn't map cleanly onto our three states, so it buckets to OTHER and is
# excluded from the discrepancy comparison rather than guessed at.
REPORTED_BUCKET_BY_CODE = {0: STATE_UNDERWAY, 1: STATE_AT_ANCHOR, 5: STATE_MOORED, 8: STATE_UNDERWAY}


def classify_raw_state(zone_type: Optional[str], sog: Optional[float]) -> str:
    if sog is not None and sog < SOG_THRESHOLD_KN:
        if zone_type == "berth":
            return STATE_MOORED
        if zone_type == "anchorage":
            return STATE_AT_ANCHOR
    return STATE_UNDERWAY


def bucket_reported_status(nav_status_code: Optional[int]) -> str:
    return REPORTED_BUCKET_BY_CODE.get(nav_status_code, "OTHER")


@dataclass
class VesselState:
    mmsi: str
    current_state: str = STATE_UNKNOWN
    state_entry_time: Optional[object] = None
    current_zone_id: Optional[str] = None
    candidate_state: Optional[str] = None
    candidate_since: Optional[object] = None
    candidate_zone_id: Optional[str] = None
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None
    last_sog: Optional[float] = None
    last_position_time: Optional[object] = None
    reported_status: Optional[str] = None
    discrepancy_since: Optional[object] = None
    discrepancy_flagged: bool = False


def process_observation(state: VesselState, obs: dict) -> list:
    """obs: {time, lat, lon, sog, zone_id, zone_type, nav_status_code}.
    Mutates `state` in place; returns a list of event dicts to emit
    (usually empty -- most observations don't cross a debounce boundary)."""
    events = []
    t = obs["time"]

    raw_state = classify_raw_state(obs.get("zone_type"), obs.get("sog"))
    zone_id = obs.get("zone_id") if raw_state != STATE_UNDERWAY else None

    if state.current_state == STATE_UNKNOWN:
        # First observation for this vessel: nothing to debounce against,
        # so adopt immediately rather than waiting out a debounce window
        # with no prior state to flap from.
        state.current_state = raw_state
        state.state_entry_time = t
        state.current_zone_id = zone_id
        state.candidate_state = None
        state.candidate_since = None
        state.candidate_zone_id = None
        events.append(_transition_event(state.mmsi, STATE_UNKNOWN, raw_state, zone_id, t))
    elif raw_state == state.current_state:
        state.candidate_state = None
        state.candidate_since = None
        state.candidate_zone_id = None
    else:
        if state.candidate_state != raw_state:
            state.candidate_state = raw_state
            state.candidate_since = t
            state.candidate_zone_id = zone_id
        elif t - state.candidate_since >= DEBOUNCE:
            events.append(_transition_event(state.mmsi, state.current_state, raw_state, zone_id, t))
            state.current_state = raw_state
            state.state_entry_time = t
            state.current_zone_id = zone_id
            state.candidate_state = None
            state.candidate_since = None
            state.candidate_zone_id = None

    state.last_lat = obs.get("lat")
    state.last_lon = obs.get("lon")
    state.last_sog = obs.get("sog")
    state.last_position_time = t

    reported_bucket = bucket_reported_status(obs.get("nav_status_code"))
    state.reported_status = reported_bucket

    if reported_bucket != "OTHER":
        if reported_bucket != state.current_state:
            if state.discrepancy_since is None:
                state.discrepancy_since = t
            elif not state.discrepancy_flagged and (t - state.discrepancy_since) >= DISCREPANCY_THRESHOLD:
                state.discrepancy_flagged = True
                events.append(_discrepancy_event(
                    state.mmsi, "discrepancy_flagged", state.current_state, reported_bucket, t
                ))
        else:
            if state.discrepancy_flagged:
                events.append(_discrepancy_event(
                    state.mmsi, "discrepancy_cleared", state.current_state, reported_bucket, t
                ))
            state.discrepancy_since = None
            state.discrepancy_flagged = False

    return events


def process_timeout(state: VesselState) -> list:
    """Called when the vessel's state has timed out (no new positions
    within TIMEOUT_AFTER of the watermark). Emits a final departure event;
    caller is responsible for actually dropping the state afterward."""
    if state.current_state == STATE_UNKNOWN:
        return []
    return [_transition_event(state.mmsi, state.current_state, "EXPIRED", None, state.last_position_time)]


def _transition_event(mmsi, from_state, to_state, zone_id, t) -> dict:
    return {
        "mmsi": mmsi,
        "event_type": "state_transition",
        "from_state": from_state,
        "to_state": to_state,
        "zone_id": zone_id,
        "derived_state": to_state,
        "reported_status": None,
        "transition_time": t,
    }


def _discrepancy_event(mmsi, event_type, derived_state, reported_status, t) -> dict:
    return {
        "mmsi": mmsi,
        "event_type": event_type,
        "from_state": None,
        "to_state": None,
        "zone_id": None,
        "derived_state": derived_state,
        "reported_status": reported_status,
        "transition_time": t,
    }
