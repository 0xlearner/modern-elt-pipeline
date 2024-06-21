FROM apache/airflow:2.9.2

# Add requirements.txt and install dependencies
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

USER root

# Copy the custom entrypoint script into the image
COPY entrypoint.sh /entrypoint.sh

# Set execute permissions for the entrypoint script
RUN chmod +x /entrypoint.sh

# Set the custom entrypoint script as the entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Install necessary tools and libraries
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    lsof \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install soda and dbt into separate virtual environments
RUN set -e \
    && python -m venv soda_venv \
    && . soda_venv/bin/activate \
    && pip install --no-cache-dir soda-core-duckdb==3.3.5 \
    && pip install --no-cache-dir soda-core-scientific==3.3.5 \
    && pip install --no-cache-dir setuptools==70.1.0 \
    && deactivate \
    && python -m venv dbt_venv \
    && . dbt_venv/bin/activate \
    && pip install --no-cache-dir dbt-core==1.8.2 \
    && pip install --no-cache-dir dbt-duckdb==1.8.1 \
    && deactivate

# Copy initialization script
COPY init_db.sh /init_db.sh
RUN chmod +x /init_db.sh

USER airflow