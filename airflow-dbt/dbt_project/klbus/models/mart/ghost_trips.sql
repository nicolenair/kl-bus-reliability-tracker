{{ config(
    materialized='incremental', 
    unique_key=['trip_id', 'dt'],
    incremental_strategy='merge') }}

with compare_planned_actual as (
    select planned_trips.route_id, planned_trips.trip_id, dt, actual_trips.trip_id as match_trip_id, actual_trips.position_date 
    from {{ ref('int_planned_trips') }} as planned_trips left join
     {{ ref('actual_trips') }} as actual_trips
    on planned_trips.trip_id = actual_trips.trip_id and dt = position_date
)

select route_id, trip_id, dt, 
case
    when match_trip_id is null then "true"
    else "false"
end as is_ghost_trip, from compare_planned_actual
