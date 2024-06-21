from airflow.decorators import dag, task
from cosmos import ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from pendulum import datetime
from pathlib import Path

from config.global_variables import global_variables as gv


DBT_PROJECT_PATH = "/opt/airflow/dags/dbt"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="retail",
    target_name="dev",
    profiles_yml_filepath=Path("/opt/airflow/dags/dbt/profiles.yml"),
)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=gv.default_args,
    description="Retail data Report",
    tags=["report", "dbt", "duckdb"],
    render_template_as_native_obj=True,
)
def report():

    report_data = DbtTaskGroup(
        group_id="report_data",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/report"],
        ),
        default_args={"pool": "duckdb"},
    )

    @task.external_python(python="/opt/airflow/soda_venv/bin/python")
    def check_report(scan_name="check_report", checks_subpath="report"):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    check_task = check_report()

    report_data >> check_task


report()
