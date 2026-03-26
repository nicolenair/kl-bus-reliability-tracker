{{ config(materialized="table")}}
select route_id, trip_id, dt from {{ ref("int_trips") }}
cross join unnest(
    generate_date_array(
        '2026-03-23',  -- your fixed min date
        date_sub((SELECT DATE(MAX(position_timestamp)) FROM {{ ref("stg_vehicle_positions") }}) , INTERVAL 1 DAY),
        interval 1 day
    )
) as dt
order by trip_id, dt