"""
Use the astro python sdk library to load data from
blob storage and write into the Snowflake warehouse.

it's using the native load process to ingest data {stage area}.

Doesn't apply any transformation during loading time.
"""


# import libraries
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
AZURE_BLOB_CONN_ID = "azure_blob_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

# default args & init dag
default_args = {
    "owner": "Everton Araujo",
    "retries": 1,
    "retry_delay": 0
}

@dag(
    dag_id="blob-stg-snowflake",
    start_date=datetime(2024, 3, 9),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    owner_links={"linkedin": "https://www.linkedin.com/in/araujoeverton/"},
    catchup=False,
    tags=['development', 'astrosdk', 'blob storage', 'snowflake']
)

# init main function
def load_data():

    # init task
    init_data_load = EmptyOperator(task_id="init")

    # TODO load data Json to warehouse
    device_data = aql.load_file(
        input_file=File("wasb://stgastropythonsdk.blob.core.windows.net/users", filetype=FileType.JSON, conn_id=AZURE_BLOB_CONN_ID),
        output_table=Table(name="users", conn_id=SNOWFLAKE_CONN_ID, metadata=Metadata(schema="astro"),),
        task_id="device_data",
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # finish task
    finish_data_load = EmptyOperator(task_id="finish")

    # define sequence
    init_data_load >> device_data >> finish_data_load


# init dag
dag = load_data()
