{{ config(materialized="table")}}
with stg_stops_mrtfeeder as (
    select * from {{ ref('stg_stops_mrtfeeder') }}), 

stg_stops as (
    select * from {{ ref('stg_stops') }})

select * from stg_stops union all select * from stg_stops_mrtfeeder