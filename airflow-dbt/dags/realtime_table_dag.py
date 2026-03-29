
from airflow.sdk import Asset, dag, task
from pendulum import datetime
from datetime import timedelta, timezone
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
from dotenv import dotenv_values
import os
from requests import get
from airflow.providers.google.cloud.hooks.gcs import parse_json_from_gcs, GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import LoadJobConfig, SourceFormat,  SchemaField, SchemaUpdateOption
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import json
from datetime import datetime

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2026, 3, 22),
    schedule="0 20 * * *",
    doc_md=__doc__,
    default_args={"owner": "Nicole", "retries": 3},
    tags=["kl-bus-reliability"],
)
def realtime_load_daily_table():
    # Define tasks
    @task(
        # Define an asset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("recently_updated_vehicle_positions")]
    )  # Define that this task updates the `current_vehicle_positions` Asset
    def get_vehicle_positions(**context) -> list[dict]:
        """
        This task pulls the latest vehicle position files from gcs.
        """
        conn_id = os.environ.get("CONN_ID")
        bucket_name = os.environ.get("GC_BUCKET_NAME")
        UTC8 = timezone(timedelta(hours=8))
        try:
            hook = GCSHook(gcp_conn_id=conn_id)
            modified_files = hook.list_by_timespan(
                bucket_name=bucket_name, 
                timespan_start=context["logical_date"] - timedelta(minutes=24*60 + 10),
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
        conn_id = os.environ.get("CONN_ID")
        bucket_name = os.environ.get("GC_BUCKET_NAME")
        project_id = os.environ.get("GCP_PROJECT_ID")

        hook = BigQueryHook(gcp_conn_id=conn_id)
        client = hook.get_client(project_id=project_id)

        schema = [
            SchemaField("timestamp", "INTEGER"),
            SchemaField("trip", "RECORD", fields=[
                SchemaField("tripId", "STRING"),
                SchemaField("startTime", "STRING"),
                SchemaField("startDate", "STRING"),
                SchemaField("routeId", "STRING"),
            ]),
            SchemaField("position", "RECORD", fields=[
                SchemaField("latitude", "FLOAT"),
                SchemaField("longitude", "FLOAT"),
                SchemaField("bearing", "FLOAT"),
                SchemaField("speed", "FLOAT"),
            ]),
            SchemaField("vehicle", "RECORD", fields=[
                SchemaField("id", "STRING"),
                SchemaField("licensePlate", "STRING"),
            ]),
        ]

        job_config = LoadJobConfig(
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            schema=schema,
            schema_update_options=[SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        ds = os.environ.get("GC_DATASET")
        load_job = client.load_table_from_uri(
            source_uris=[f"gs://{bucket_name}/{filename}" for filename in modified_files],
            destination=f"{ds}.rtdump",
            job_config=job_config,
        )

        return str(load_job.result())  # wait for job to complete
    
    env_vars = env_vars = {k: v for k, v in os.environ.items() if '{' not in str(v) and '}' not in str(v)}

    print(env_vars.keys())

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-custom",
        command="dbt run --project-dir /dbt_project/klbus --profiles-dir /root/.dbt",
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow-dbt_data_pipeline",
        auto_remove="success",
        environment=env_vars,  # already handled by env_file below
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=os.environ.get("DBT_PROJECT_DIR"),  # replace with your PWD equivalent
                target="/dbt_project/klbus",
                type="bind",
            ),
            Mount(
                source=os.environ.get("DBT_PROFILES_DIR"),
                target="/root/.dbt",
                type="bind",
            ),
            Mount(
                source=os.environ.get("DBT_KEYFILE_PATH"),
                target="/kl-bus-reliability-tracker-25984de72887.json",
                type="bind",
            ),
        ],
    )
    # disable Jinja templating on the environment field
    dbt_run.template_fields = tuple(
        f for f in dbt_run.template_fields if f != "environment"
    )



    vp = get_vehicle_positions()
    load_str = write_vehicle_positions_to_bigquery(vp)
    load_str >> dbt_run


# Instantiate the DAG
realtime_load_daily_table()
