-- Pairs each discrepancy_flagged with the next discrepancy_cleared for
-- that vessel (README: the state machine only ever tracks one active
-- discrepancy per mmsi, so "next cleared after this flagged" is
-- unambiguous, not a guess). cleared_at null + is_ongoing means the
-- discrepancy hasn't resolved as of the last processed event.
with flags as (
    select mmsi, transition_time as flagged_at, derived_state, reported_status
    from {{ ref('stg_vessel_state_changes') }}
    where event_type = 'discrepancy_flagged'
),

clears as (
    select mmsi, transition_time as cleared_at
    from {{ ref('stg_vessel_state_changes') }}
    where event_type = 'discrepancy_cleared'
),

paired as (
    select
        f.mmsi,
        f.flagged_at,
        f.derived_state,
        f.reported_status,
        min(c.cleared_at) as cleared_at
    from flags f
    left join clears c
        on f.mmsi = c.mmsi
        and c.cleared_at > f.flagged_at
    group by f.mmsi, f.flagged_at, f.derived_state, f.reported_status
)

select
    md5(mmsi || '|' || cast(flagged_at as varchar)) as discrepancy_event_id,
    mmsi,
    derived_state,
    reported_status,
    flagged_at,
    cleared_at,
    date_diff('second', flagged_at, coalesce(cleared_at, current_timestamp)) as duration_seconds,
    cleared_at is null as is_ongoing
from paired
