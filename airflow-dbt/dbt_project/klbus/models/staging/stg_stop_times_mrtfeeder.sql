{{ config(materialized='table')}}
with source as (
    select * from {{ source('raw', 'stop_times_mrtfeeder') }}
)

select
    trip_id,
    stop_id,
    stop_sequence,
    stop_headsign,
    arrival_time,
    departure_time
from source