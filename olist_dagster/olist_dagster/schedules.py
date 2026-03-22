from dagster_dbt import build_schedule_from_dbt_selection
from .assets import olist_pipeline_dbt_assets

schedules = [
    build_schedule_from_dbt_selection(
        [olist_pipeline_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 6 * * *",  # runs daily at 6am
        dbt_select="fqn:*",
    ),
]