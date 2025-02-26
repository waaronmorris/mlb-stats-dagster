from typing import Any, Mapping, Optional

import dagster as dg
from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator
from dagster_dbt import DbtCliResource, dbt_assets

from mlb_stats.project import mlb_stats_dbt_project


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        # Use the custom asset key from the dbt metadata if available
        key_prefix = dbt_resource_props.get('meta', {}).get('dagster', {}).get('key_prefix', [])
        asset_key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props["resource_type"] == "source":
            asset_key = asset_key.with_prefix(key_prefix)
        return asset_key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        # Use the custom group from the dbt metadata if available
        custom_group = dbt_resource_props.get('meta', {}).get('dagster', {}).get('group')
        if custom_group:
            return custom_group
        return super().get_group_name(dbt_resource_props)

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        # Use the custom tags from the dbt metadata if available
        custom_tags = dbt_resource_props.get('meta', {}).get('dagster', {}).get('tags', {})
        return {**super().get_tags(dbt_resource_props), **custom_tags}


@dbt_assets(
    manifest=mlb_stats_dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def mlb_stats_dbt_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()