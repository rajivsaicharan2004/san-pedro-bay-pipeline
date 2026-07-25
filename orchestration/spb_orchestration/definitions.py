from dagster import Definitions

from .assets import dbt_resource, spb_dbt_assets
from .jobs import spb_dbt_build_job
from .schedules import spb_dbt_daily_backstop_schedule
from .sensors import lakehouse_sync_sensor

defs = Definitions(
    assets=[spb_dbt_assets],
    jobs=[spb_dbt_build_job],
    schedules=[spb_dbt_daily_backstop_schedule],
    sensors=[lakehouse_sync_sensor],
    resources={"dbt": dbt_resource},
)
