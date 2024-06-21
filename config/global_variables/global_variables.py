# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from minio import Minio


SOURCE_DATA_PATH = os.getenv("SOURCE_DATA_PATH")


MINIO_REGION = os.getenv("MINIO_REGION")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL_STYLE = os.getenv("MINIO_URL_STYLE")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL")
BUCKET_NAME = os.getenv("BUCKET_NAME")
ARCHIVE_BUCKET_NAME = os.getenv("ARCHIVE_BUCKET_NAME")
DUCKDB_CONN_ID = os.getenv("AIRFLOW_CONN_DUCKDB_ID")
DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_PATH")


# Datasets
DS_START = Dataset("start")
DS_DATA_MINIO = Dataset(f"minio://{BUCKET_NAME}")
DS_DUCKDB_RETAIL_DATA = Dataset("duckdb://retail_data")

# get Airflow task logger
task_log = logging.getLogger("airflow.task")


# DAG default arguments
default_args = {
    "owner": "zeroxlearner",
    "depends_on_past": False,
    "retries": 2,
}


# utility functions
def get_minio_client():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client