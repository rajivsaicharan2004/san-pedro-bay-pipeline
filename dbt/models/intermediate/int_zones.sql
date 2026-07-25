-- vessel_state_changes carries zone_id but not zone_type/zone_name (see
-- vessel_state_job.py's OUTPUT_SCHEMA) -- positions_silver has all three,
-- so this is the lookup that lets int_vessel_sessions attach zone_name
-- without a separate seed for data that's already in the lake.
select distinct
    zone_id,
    zone_type,
    zone_name
from {{ ref('stg_positions_silver') }}
where zone_id is not null
