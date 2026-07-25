-- Approximation, not hidden: a session spanning midnight is attributed
-- entirely to its start date rather than split across both days. Fine
-- for a first cut at daily activity rollups; revisit if a report ever
-- needs exact per-day duration splits.
select
    mmsi,
    cast(session_start as date) as activity_date,
    state,
    count(*) as session_count,
    sum(session_duration_seconds) as total_duration_seconds
from {{ ref('int_vessel_sessions') }}
where session_end is not null
group by mmsi, cast(session_start as date), state
