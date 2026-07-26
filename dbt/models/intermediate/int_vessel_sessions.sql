-- Turns point-in-time state_transition events into intervals: each row is
-- one continuous period a vessel spent in a given state/zone, from the
-- transition that entered it to the transition that left it. The last
-- transition per vessel has no "next" yet -- session_end is null and
-- is_ongoing is true, not a bug, that vessel just hasn't left the state.
with state_transitions as (
    -- distinct: a crash-and-restart of vessel_state_job (Kafka producer
    -- side-effects inside foreachBatch are at-least-once, not exactly-
    -- once, even though the Delta write itself is) can replay the exact
    -- same (mmsi, to_state, transition_time) transition twice. Real,
    -- observed locally from an earlier dev run, not hypothetical --
    -- collapsing exact duplicates here is defensive, cheap, and correct
    -- regardless of why a duplicate arose.
    select distinct
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
-- EXPIRED (vessel_state_logic.process_timeout) marks that this mmsi's
-- watermark session timed out -- it's not a real physical state, but its
-- transition_time still had to flow through the lead() above to
-- correctly close out the real session before it (tracking stopping is
-- as much an "end" as changing state is). Filtering here, after that
-- computation, rather than out of state_transitions, is what makes that
-- work: filtering earlier would make the prior real session look
-- falsely ongoing.
where sessions.state != 'EXPIRED'
