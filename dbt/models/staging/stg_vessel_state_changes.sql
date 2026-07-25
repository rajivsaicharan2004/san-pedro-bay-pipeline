select
    mmsi,
    event_type,
    from_state,
    to_state,
    zone_id,
    derived_state,
    reported_status,
    transition_time
from {{ source('lakehouse', 'vessel_state_changes') }}
