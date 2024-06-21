"""DAG that loads online_retail ingests from local csv file into MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import io

# -------------------- #
# Local module imports #
# -------------------- #


from config.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operator.minio import LocalFilesystemToMinIOOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2024, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Ingests data from provided csv files to MinIO.",
    tags=["ingestion", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def data_ingestion():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(task_id="create_bucket", bucket_name=gv.BUCKET_NAME)

    # use the custom LocalCSVToMinIOOperator to read the contents in /include/climate
    # into MinIO. This task uses dynamic task allowing you to add additional files to
    # the folder and reading them in without changing any DAG code
    ingest_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_data",
        bucket_name=gv.BUCKET_NAME,
        outlets=[gv.DS_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.SOURCE_DATA_PATH,
                "object_name": gv.SOURCE_DATA_PATH.split("/")[-1],
            },
        ]
    )

    # set dependencies
    create_bucket_tg >> ingest_data


data_ingestion()
