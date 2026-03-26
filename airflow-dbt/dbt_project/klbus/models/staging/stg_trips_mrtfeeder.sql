{{ config(materialized='table')}}
with source as (
    select * from {{ source('raw', 'trips_mrtfeeder') }}
)

select
    trip_id,
    cast(route_id as string) as route_id
from source