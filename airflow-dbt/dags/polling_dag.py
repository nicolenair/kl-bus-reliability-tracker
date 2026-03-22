
from airflow.sdk import Asset, dag, task
from pendulum import datetime
from datetime import timedelta
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
from requests import get
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
from datetime import datetime
import os

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 4, 22),
    schedule=timedelta(seconds=30),
    doc_md=__doc__,
    default_args={"owner": "Nicole", "retries": 3},
    tags=["kl-bus-reliability"],
)
def realtime_poll():
    # Define tasks
    @task(
        # Define an asset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("current_vehicle_positions")]
    )  # Define that this task updates the `current_vehicle_positions` Asset
    def get_vehicle_positions(**context) -> list[dict]:
        """
        This task uses the API to retrieve the current vehicle positions.
        """
        try:
            # Sample GTFS-R URL from Malaysia's Open API
            URLS = ['https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-kl'] #,'https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-mrtfeeder'
            
            vehicle_positions = []
            for URL in URLS:
                # Parse the GTFS Realtime feed
                feed = gtfs_realtime_pb2.FeedMessage()
                response = get(URL)
                feed.ParseFromString(response.content)
                
                # Extract and print vehicle position information
                vehicle_positions.extend([MessageToDict(entity.vehicle) for entity in feed.entity])
            # vehicle_positions = [{'trip': {'tripId': 'TEST', 'routeId': 'TEST'}, 'vehicle': {'id': 'TEST', 'licensePlate': 'TEST'}, 'position': {'speed': 13.0, 'bearing': 274.0, 'latitude': 3.206444, 'longitude': 101.58082}, 'timestamp': '1886017556'}]
            print(f'Total vehicles: {len(vehicle_positions)}')
            return vehicle_positions
        except Exception:
            print("API currently not available, using hardcoded data instead.")
            vehicle_positions = [{'trip': {'tripId': 'TEST', 'routeId': 'TEST'}, 'vehicle': {'id': 'TEST', 'licensePlate': 'TEST'}, 'position': {'speed': 13.0, 'bearing': 274.0, 'latitude': 3.206444, 'longitude': 101.58082}, 'timestamp': '1886017556'}]

        return vehicle_positions

    @task
    def write_vehicle_positions_to_bucket(vehicle_positions_df: list, **context) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        

        hook = GCSHook(gcp_conn_id=os.getenv("CONN_ID"))

        # Or write a string directly (no local file needed)
        timestamp = context["logical_date"]
        hook.upload(
            bucket_name=os.getenv("GC_BUCKET_NAME"),
            object_name=f"realtime_poll_json/{timestamp}.json",
            data="\n".join(json.dumps(r) for r in vehicle_positions_df),              # raw string data
        )

    vp = get_vehicle_positions()
    write_vehicle_positions_to_bucket(vp)


# Instantiate the DAG
realtime_poll()
