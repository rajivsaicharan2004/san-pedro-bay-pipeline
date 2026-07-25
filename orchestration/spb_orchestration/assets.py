"""Wraps the dbt project (../../dbt) as Dagster asset. dbt runs in its own
venv (dbt/.venv), separate from Dagster's -- DbtCliResource is pointed at
that venv's dbt executable explicitly rather than relying on it being on
PATH, since these two venvs are never activated together in the systemd
units (see infra/oci/systemd/spb-dagster-*.service).

Freshness policies on the three staging models mirror the thresholds
already declared as dbt source freshness in dbt/models/staging/_sources.yml
-- same numbers, so Dagster's asset health view and `dbt source freshness`
never disagree about what "stale" means for the same table.
"""
from datetime import timedelta
from pathlib import Path

from dagster import AssetExecutionContext, FreshnessPolicy
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

REPO_ROOT = Path(__file__).parent.parent.parent.resolve()
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"
DBT_EXECUTABLE = str(DBT_PROJECT_DIR / ".venv" / "bin" / "dbt")

STAGING_FRESHNESS_POLICIES = {
    "stg_positions_silver": FreshnessPolicy.time_window(
        warn_window=timedelta(minutes=15), fail_window=timedelta(minutes=30)
    ),
    "stg_vessel_state_changes": FreshnessPolicy.time_window(
        warn_window=timedelta(minutes=15), fail_window=timedelta(minutes=30)
    ),
    # Ships broadcast static data far less often than position reports --
    # see the matching comment in dbt/models/staging/_sources.yml.
    "stg_ship_static": FreshnessPolicy.time_window(
        warn_window=timedelta(minutes=60), fail_window=timedelta(minutes=180)
    ),
}


class SPBDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        dbt_resource_props = manifest["nodes"].get(unique_id) or manifest["sources"].get(unique_id)
        model_name = dbt_resource_props["name"] if dbt_resource_props else None
        policy = STAGING_FRESHNESS_POLICIES.get(model_name)
        if policy is not None:
            spec = spec.replace_attributes(freshness_policy=policy)
        return spec


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=SPBDagsterDbtTranslator(),
)
def spb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    dbt_executable=DBT_EXECUTABLE,
)
