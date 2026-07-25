from dagster import AssetSelection, define_asset_job

from .assets import spb_dbt_assets

spb_dbt_build_job = define_asset_job(
    "spb_dbt_build_job",
    selection=AssetSelection.assets(spb_dbt_assets),
)
