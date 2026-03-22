from pathlib import Path
from dagster_dbt import DbtProject

olist_pipeline_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
    profiles_dir=Path.home() / ".dbt",
)
olist_pipeline_project.prepare_if_dev()