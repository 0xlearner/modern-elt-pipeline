#!/bin/bash
set -e

# Add the necessary directories to the PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/opt/airflow/config:/opt/airflow/include:/opt/airflow/dags:/opt/airflow/plugins

# Adjust permissions for the mounted volume
# chown default:root /opt/airflow/include/DWH
# chmod 660 /opt/airflow/include/DWH

# Execute the original entrypoint script
exec /entrypoint "$@"