from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from mlb_stats.project import mlb_stats_dbt_project


@dbt_assets(manifest=mlb_stats_dbt_project.manifest_path)
def mlb_stats_dbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


