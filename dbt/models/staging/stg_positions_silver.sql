select
    mmsi,
    event_time,
    event_date,
    lat,
    lon,
    sog,
    nav_status_code,
    zone_id,
    zone_type,
    zone_name,
    ingested_at
from {{ source('lakehouse', 'positions_silver') }}
