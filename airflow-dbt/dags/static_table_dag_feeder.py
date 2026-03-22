
from airflow.sdk import Asset, dag, task
from pendulum import datetime
from datetime import timedelta, timezone
import requests
import os
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
from requests import get
from airflow.providers.google.cloud.hooks.gcs import parse_json_from_gcs, GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import LoadJobConfig, SourceFormat
import requests
import zipfile
import io

import json
from datetime import datetime

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 4, 22),
    schedule="0 20 * * *",
    doc_md=__doc__,
    default_args={"owner": "Nicole", "retries": 3},
    tags=["kl-bus-reliability"],
)
def static_load():
    # Define tasks
    @task(
        # Define an asset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("route_info")]
    )
    def upload_daily_route_info(**context) -> list[dict]:
        """
        This task pulls the latest route info from the GTFS static API.
        """
        url = "https://api.data.gov.my/gtfs-static/prasarana?category=rapid-bus-mrtfeeder"

        # Download
        response = requests.get(url)
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))

        hook = GCSHook(gcp_conn_id=os.getenv("CONN_ID"))

        timestamp = context["logical_date"]

        objs = {}
        for name in zip_file.namelist():
            with zip_file.open(name) as f:
                object_name = f"daily_routes_feeder/{timestamp}_{name}"
                hook.upload(
                    bucket_name=os.getenv("GC_BUCKET_NAME"),
                    object_name=object_name,
                    data=pd.read_csv(f).to_csv(),              # raw string data
                )
                objs[name.replace(".txt", "") + "_mrtfeeder"] = object_name
        return objs

    @task
    def load_route_info_to_bigquery(objs) -> None:
        conn_id = os.getenv("CONN_ID")
        bucket_name = os.getenv("GC_BUCKET_NAME")
        project_id = os.getenv("GC_PROJECT_ID")

        hook = BigQueryHook(gcp_conn_id=conn_id)
        client = hook.get_client(project_id=project_id)

        job_config = LoadJobConfig(
            source_format=SourceFormat.CSV,
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )
        
        ds = os.getenv("GC_DATASET")
        for table_nm in objs:
            load_job = client.load_table_from_uri(
                source_uris=[f"gs://{bucket_name}/{objs[table_nm]}"],
                destination=f"{ds}.{table_nm}",
                job_config=job_config,
            )

        return str(load_job.result())  # wait for job to complete

    vp = upload_daily_route_info()
    load_route_info_to_bigquery(vp)


# Instantiate the DAG
static_load()
