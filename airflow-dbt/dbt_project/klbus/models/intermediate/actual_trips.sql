{{ config(materialized="table")}}
SELECT DISTINCT DATE(position_timestamp) as position_date, trip_id 
FROM {{ ref("stg_vehicle_positions") }}