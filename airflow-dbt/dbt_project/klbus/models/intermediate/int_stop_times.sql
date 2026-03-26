{{ config(materialized="table")}}
with stg_stop_times_mrtfeeder as (
    select * from {{ ref('stg_stop_times_mrtfeeder') }}), 

stg_stop_times as (
    select * from {{ ref('stg_stop_times') }})

select * from stg_stop_times union all select * from stg_stop_times_mrtfeeder