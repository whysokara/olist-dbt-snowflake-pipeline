from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import olist_pipeline_project


@dbt_assets(manifest=olist_pipeline_project.manifest_path)
def olist_pipeline_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    