from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import olist_pipeline_dbt_assets
from .project import olist_pipeline_project
from .schedules import schedules
from pathlib import Path

defs = Definitions(
    assets=[olist_pipeline_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(
            project_dir=olist_pipeline_project,
            profiles_dir=Path.home() / ".dbt",
        ),
    },
)