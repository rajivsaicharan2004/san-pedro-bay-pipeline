-- ship_static is an append-only log of every ShipStaticData message seen
-- (see ship_static_job.py's docstring on why there's no upsert) -- this
-- is where "latest known values per mmsi" actually gets resolved.
with ranked as (
    select
        *,
        row_number() over (partition by mmsi order by ingested_at desc) as rn
    from {{ ref('stg_ship_static') }}
)

select
    mmsi,
    -- AIS Type 5 names/destinations are fixed-width fields, padded with
    -- trailing spaces on the wire -- confirmed against real data, not
    -- hypothetical.
    trim(ship_name) as ship_name,
    trim(call_sign) as call_sign,
    ship_type_code,
    trim(destination) as destination,
    imo_number,
    max_static_draught,
    ingested_at as last_seen_at
from ranked
where rn = 1
