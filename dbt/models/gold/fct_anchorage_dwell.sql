-- The port-congestion KPI: how long a vessel waits at anchor, and whether
-- it went on to actually berth afterward (proceeded_to_berth = false can
-- mean it departed back out to sea instead -- a materially different
-- outcome than a normal anchor-then-berth cycle).
with anchor_sessions as (
    select *
    from {{ ref('int_vessel_sessions') }}
    where state = 'AT_ANCHOR'
      and zone_type = 'anchorage'
),

next_session as (
    select
        mmsi,
        session_start,
        lead(state) over (partition by mmsi order by session_start) as next_state,
        lead(zone_type) over (partition by mmsi order by session_start) as next_zone_type
    from {{ ref('int_vessel_sessions') }}
)

select
    a.session_id as anchorage_dwell_id,
    a.mmsi,
    a.zone_id as anchorage_id,
    a.zone_name as anchorage_name,
    a.session_start as arrival_time,
    a.session_end as departure_time,
    a.session_duration_seconds as dwell_seconds,
    a.is_ongoing,
    (n.next_state = 'MOORED' and n.next_zone_type = 'berth') as proceeded_to_berth
from anchor_sessions a
left join next_session n
    on a.mmsi = n.mmsi
    and a.session_start = n.session_start
