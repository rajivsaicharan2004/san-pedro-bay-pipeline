from dagster import ScheduleDefinition

from .jobs import spb_dbt_build_job

# Backstop, not the primary trigger -- lakehouse_sync_sensor (sensors.py)
# is what normally drives materialization. 06:00 UTC is off-peak for both
# AIS traffic patterns and the retry_apply.sh-adjacent OCI API usage this
# box already does.
spb_dbt_daily_backstop_schedule = ScheduleDefinition(
    job=spb_dbt_build_job,
    cron_schedule="0 6 * * *",
)
