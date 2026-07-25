select
    session_id as berth_visit_id,
    mmsi,
    zone_id as berth_id,
    zone_name as berth_name,
    session_start as arrival_time,
    session_end as departure_time,
    session_duration_seconds,
    is_ongoing
from {{ ref('int_vessel_sessions') }}
where state = 'MOORED'
  and zone_type = 'berth'
