
from airflow.sdk import Asset, dag, task
from pendulum import datetime
from datetime import timedelta, timezone
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
import os
from requests import get
from airflow.providers.google.cloud.hooks.gcs import parse_json_from_gcs, GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import LoadJobConfig, SourceFormat

import json
from datetime import datetime

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 4, 22),
    schedule=timedelta(minutes=5),
    doc_md=__doc__,
    default_args={"owner": "Nicole", "retries": 3},
    tags=["kl-bus-reliability"],
)
def realtime_load():
    # Define tasks
    @task(
        # Define an asset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("recently_updated_vehicle_positions")]
    )  # Define that this task updates the `current_vehicle_positions` Asset
    def get_vehicle_positions(**context) -> list[dict]:
        """
        This task pulls the latest vehicle position files from gcs.
        """
        conn_id = os.getenv("CONN_ID")
        bucket_name = os.getenv("GC_BUCKET_NAME")
        UTC8 = timezone(timedelta(hours=8))
        try:
            hook = GCSHook(gcp_conn_id=conn_id)
            modified_files = hook.list_by_timespan(
                bucket_name=bucket_name, 
                timespan_start=context["logical_date"] - timedelta(minutes=5),
                timespan_end=context["logical_date"], 
                prefix="realtime_poll_json/"
            )
            return modified_files
            # for filename in modified_files:
            #     vehicle_positions.extend(parse_json_from_gcs(conn_id, f"gs://{bucket_name}/{filename}"))
        except Exception as e:
            print("API currently not available, using hardcoded data instead.", str(e))
            # vehicle_positions = [{'trip': {'tripId': 'TEST', 'routeId': 'TEST'}, 'vehicle': {'id': 'TEST', 'licensePlate': 'TEST'}, 'position': {'speed': 13.0, 'bearing': 274.0, 'latitude': 3.206444, 'longitude': 101.58082}, 'timestamp': '1886017556'}]
            modified_files = []

        return modified_files

    @task
    def write_vehicle_positions_to_bigquery(modified_files: list) -> None:
        conn_id = os.getenv("CONN_ID")
        bucket_name = os.getenv("GC_BUCKET_NAME")
        project_id = os.getenv("GC_PROJECT_ID")

        hook = BigQueryHook(gcp_conn_id=conn_id)
        client = hook.get_client(project_id=project_id)

        job_config = LoadJobConfig(
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )
        
        ds = os.getenv("GC_DATASET")
        load_job = client.load_table_from_uri(
            source_uris=[f"gs://{bucket_name}/{filename}" for filename in modified_files],
            destination=f"{ds}.rtdump",
            job_config=job_config,
        )

        return str(load_job.result())  # wait for job to complete

    vp = get_vehicle_positions()
    write_vehicle_positions_to_bigquery(vp)


# Instantiate the DAG
realtime_load()
