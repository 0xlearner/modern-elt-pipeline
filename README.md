# modern-elt-pipeline

![elt diagram](https://github.com/0xlearner/modern-elt-pipeline/blob/main/images/elt_diagram.png)

The pipeline begins by extracting data from a local file system CSV file and loading it into Minio, an open-source object storage solution. Since Airflow doesn't have a built-in operator for Minio, I have to use a custom Minio operator using Airflow's TaskGroup functionality, which allows us to encapsulate multiple tasks into a single logical unit. This custom operator handles various operations, such as listing bucket files, checking for files, and uploading multiple files.

Once the data is loaded into Minio S3, we extract it and load it into a local data warehouse, DuckDB, using Airflow and raw SQL queries. DuckDB is a lightweight, embeddable SQL data analytics solution that provides efficient analytical capabilities.

To ensure data quality, we utilize Soda, a powerful data quality tool, to perform basic checks on the loaded data. These checks verify that the data contains all the expected columns and adheres to the appropriate schema and data types.

After loading the data into the data warehouse, we move on to the data modeling stage, where we leverage dbt (Data Build Tool), a popular transformation tool in the ELT process. We have two options for running dbt models: either using the traditional Bash operator in Airflow or using the more organized approach with Astronomer's Cosmos package and DbtTaskGroup. We chose the latter option, which provides better visibility into model lineage and dependency management.

Once the data is transformed using dbt, we perform additional data quality checks with Soda to ensure the integrity of the transformed data.

To start the you just need to trigger the ```start``` dag manually which will produce the Dataset which will trgger the rest of the pipeline.

![airflow](https://github.com/0xlearner/modern-elt-pipeline/blob/main/images/2024-06-20_16-15-10.png)

With the dimension and fact tables generated, we create simple reports based on the data, such as the top 10 products by quantity sold, total revenue per month, and primary markets. To visualize these reports, we utilize Metabase, a powerful open-source business intelligence tool, to create an interactive dashboard for data visualization.
![metabase](https://github.com/0xlearner/modern-elt-pipeline/blob/main/images/Screenshot%20from%202024-06-21%2015-02-15.png)

