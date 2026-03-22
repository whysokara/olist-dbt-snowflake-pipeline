from setuptools import find_packages, setup

setup(
    name="olist_dagster",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "olist_dagster": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)