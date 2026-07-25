"""Fires the dbt build shortly after infra/oci/scripts/sync_lakehouse.sh
(systemd timer, every 5 minutes) pulls fresh data down from the bucket --
this is what makes materialization data-driven instead of purely
schedule-driven. schedules.py's daily job is the backstop for whenever
this sensor doesn't fire (Dagster daemon down, sync timer stalled, etc.).

Checking one marker file's mtime instead of walking every Delta file
under the sync dir is deliberate -- this runs every 30s by default.
"""
import os

from dagster import DefaultSensorStatus, RunRequest, SensorEvaluationContext, SkipReason, sensor

from .jobs import spb_dbt_build_job

LAKEHOUSE_SYNC_DIR = os.getenv("LAKEHOUSE_SYNC_DIR", "/home/ubuntu/lakehouse_sync")
SYNC_MARKER = os.path.join(LAKEHOUSE_SYNC_DIR, ".last_synced")


@sensor(job=spb_dbt_build_job, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def lakehouse_sync_sensor(context: SensorEvaluationContext):
    if not os.path.exists(SYNC_MARKER):
        return SkipReason(f"{SYNC_MARKER} not found yet -- lakehouse sync hasn't run.")

    mtime = os.path.getmtime(SYNC_MARKER)
    last_seen = float(context.cursor) if context.cursor else 0.0

    if mtime <= last_seen:
        return SkipReason("No sync since the last run.")

    context.update_cursor(str(mtime))
    return RunRequest(run_key=str(mtime))
