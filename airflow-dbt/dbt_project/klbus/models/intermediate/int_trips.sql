{{ config(materialized="table")}}
with stg_trips_mrtfeeder as (
    select * from {{ ref('stg_trips_mrtfeeder') }}), 

stg_trips as (
    select * from {{ ref('stg_trips') }})

select * from stg_trips union all select * from stg_trips_mrtfeeder