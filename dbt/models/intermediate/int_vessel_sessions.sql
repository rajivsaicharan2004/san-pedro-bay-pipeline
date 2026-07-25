-- Turns point-in-time state_transition events into intervals: each row is
-- one continuous period a vessel spent in a given state/zone, from the
-- transition that entered it to the transition that left it. The last
-- transition per vessel has no "next" yet -- session_end is null and
-- is_ongoing is true, not a bug, that vessel just hasn't left the state.
with state_transitions as (
    select
        mmsi,
        to_state as state,
        zone_id,
        transition_time as session_start
    from {{ ref('stg_vessel_state_changes') }}
    where event_type = 'state_transition'
),

sessions as (
    select
        mmsi,
        state,
        zone_id,
        session_start,
        lead(session_start) over (partition by mmsi order by session_start) as session_end
    from state_transitions
)

select
    md5(sessions.mmsi || '|' || cast(sessions.session_start as varchar)) as session_id,
    sessions.mmsi,
    sessions.state,
    sessions.zone_id,
    zones.zone_type,
    zones.zone_name,
    sessions.session_start,
    sessions.session_end,
    date_diff('second', sessions.session_start, coalesce(sessions.session_end, current_timestamp)) as session_duration_seconds,
    sessions.session_end is null as is_ongoing
from sessions
left join {{ ref('int_zones') }} as zones
    on sessions.zone_id = zones.zone_id
