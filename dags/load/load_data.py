"""DAG that loads raw retail data from MinIO to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime, parse
import duckdb
import os
import json

# -------------------- #
# Local module imports #
# -------------------- #

from config.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operator.minio import (
    MinIOListOperator,
    MinIOCopyObjectOperator,
    MinIODeleteObjectsOperator,
)


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in MinIO
    schedule=[gv.DS_DATA_MINIO],
    catchup=False,
    default_args=gv.default_args,
    description="Loads retail data from MinIO to DuckDB.",
    tags=["load", "minio", "duckdb"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def load_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_archive_bucket", bucket_name=gv.ARCHIVE_BUCKET_NAME
    )

    list_files_bucket = MinIOListOperator(
        task_id="list_files_bucket", bucket_name=gv.BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_RETAIL_DATA], pool="duckdb")
    def load_retail_data(obj):
        """Loads content of one fileobject in the MinIO bucket
        to DuckDB."""

        # derive table name from object name
        table_name = obj.split(".")[0] + "_table"

        # connect to DuckDB
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)

        # Load the HTTPFS extension
        cursor.execute("INSTALL httpfs;")
        cursor.execute("LOAD httpfs;")

        # Load the S3 configurations
        cursor.execute(f"SET s3_region='{gv.MINIO_REGION}';")
        cursor.execute(f"SET s3_endpoint='{gv.MINIO_ENDPOINT}';")
        cursor.execute(f"SET s3_access_key_id='{gv.MINIO_ACCESS_KEY}';")
        cursor.execute(f"SET s3_secret_access_key='{gv.MINIO_SECRET_KEY}';")
        cursor.execute(f"SET s3_url_style='{gv.MINIO_URL_STYLE}';")
        cursor.execute(
            f"SET s3_use_ssl={gv.MINIO_USE_SSL};"
        )  # Disable SSL verification

        # Define the URL of the CSV file
        s3_url = f"s3://{gv.BUCKET_NAME}/{obj}"
        # Create a table and load data in a single step using CREATE TABLE AS
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS raw_{table_name} AS 
        SELECT * 
        FROM read_csv_auto('{s3_url}', AUTO_DETECT=TRUE, IGNORE_ERRORS=TRUE);
        """
        cursor.execute(create_table_query)
        cursor.commit()
        cursor.close()

    @task
    def get_copy_args(obj_list):
        """Return tuples with bucket names and bucket contents."""

        return [
            {
                "source_bucket_name": gv.BUCKET_NAME,
                "source_object_names": obj_list,
                "dest_object_names": obj_list,
            },
        ]

    copy_objects_to_archive = MinIOCopyObjectOperator.partial(
        task_id="copy_objects_to_archive",
        dest_bucket_name=gv.ARCHIVE_BUCKET_NAME,
    ).expand_kwargs(get_copy_args(list_files_bucket.output))

    @task
    def get_deletion_args(obj_list):
        """Return tuples with bucket names and bucket contents."""

        return [
            {"bucket_name": gv.BUCKET_NAME, "object_names": obj_list},
        ]

    delete_objects = MinIODeleteObjectsOperator.partial(
        task_id="delete_objects",
    ).expand_kwargs(get_deletion_args(list_files_bucket.output))

    @task.external_python(python="/opt/airflow/soda_venv/bin/python")
    def check_load(scan_name="check_load", checks_subpath="sources"):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    # set dependencies

    retail_data = load_retail_data.expand(obj=list_files_bucket.output)

    archive_bucket = create_bucket_tg

    check_task = check_load()

    retail_data >> check_task >> archive_bucket
    (archive_bucket >> [copy_objects_to_archive] >> delete_objects)


load_data()
