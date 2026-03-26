{{ config(materialized='table')}}
with source as (
    select * from {{ source('raw', 'stops_mrtfeeder') }}
)

select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from source