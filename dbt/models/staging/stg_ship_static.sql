select
    mmsi,
    ship_name,
    call_sign,
    ship_type_code,
    destination,
    imo_number,
    max_static_draught,
    ingested_at
from {{ source('lakehouse', 'ship_static') }}
