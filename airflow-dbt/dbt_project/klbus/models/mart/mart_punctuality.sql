{{ config(
    materialized='incremental', 
    unique_key=['position_timestamp', 'trip_id', 'stop_id'],
    incremental_strategy='merge', 
    partition_by={
      "field": "actual_arrival_time",
      "data_type": "datetime",
      "granularity": "day"
    },
    cluster_by=["route_id"]
    ) }}
with expanded_veh_positions as (
    select svp.position_timestamp as position_timestamp, svp.trip_id as trip_id, route_id, 
    latitude, longitude, vehicle_id, vehicle_license_plate, bearing, speed, 
    stop_times_with_coordinates.stop_id as 
    stop_id, stop_name, stop_lat, stop_lon, 
    position_date,
    actual_arrival_time,
    DATETIME_ADD(
        DATETIME(TIMESTAMP(CAST(
            DATE_SUB(
                position_date,
                INTERVAL (
                    CASE WHEN CAST(SPLIT(arrival_time, ':')[OFFSET(0)] AS INT64) >= 24
                    THEN 1 ELSE 0 END
                ) DAY
            ) AS STRING
        ))),
        INTERVAL (
            CAST(SPLIT(arrival_time, ':')[OFFSET(0)] AS INT64) * 3600 +
            CAST(SPLIT(arrival_time, ':')[OFFSET(1)] AS INT64) * 60 +
            CAST(SPLIT(arrival_time, ':')[OFFSET(2)] AS INT64)
        ) SECOND
    ) AS planned_arrival_time,    
    DATETIME_ADD(
        DATETIME(TIMESTAMP(CAST(
            DATE_SUB(
                position_date,
                INTERVAL (
                    CASE WHEN CAST(SPLIT(departure_time, ':')[OFFSET(0)] AS INT64) >= 24
                    THEN 1 ELSE 0 END
                ) DAY
            ) AS STRING
        ))),
        INTERVAL (
            CAST(SPLIT(departure_time, ':')[OFFSET(0)] AS INT64) * 3600 +
            CAST(SPLIT(departure_time, ':')[OFFSET(1)] AS INT64) * 60 +
            CAST(SPLIT(departure_time, ':')[OFFSET(2)] AS INT64)
        ) SECOND
    ) as planned_departure_time, stop_sequence  FROM 
    {{ ref("stg_vehicle_positions") }}
    as svp JOIN {{ ref("int_stop_times_with_coordinates") }} as stop_times_with_coordinates
    ON svp.trip_id = stop_times_with_coordinates.trip_id), 

min_distance_table as (
    select trip_id, stop_id, position_date, 
    min(ST_DISTANCE(ST_GEOGPOINT(expanded_veh_positions.longitude, expanded_veh_positions.latitude), 
    ST_GEOGPOINT(stop_lon, stop_lat))) as md
    from expanded_veh_positions 
    group by 
    expanded_veh_positions.route_id, 
    expanded_veh_positions.trip_id, 
    expanded_veh_positions.stop_id, 
    position_date
    ),

punctuality_1 as (select 
    position_timestamp,
    expanded_veh_positions.actual_arrival_time, 
    planned_arrival_time,
    planned_departure_time,
    expanded_veh_positions.position_date, route_id, expanded_veh_positions.trip_id, 
    expanded_veh_positions.stop_id, stop_sequence, 
    expanded_veh_positions.longitude, expanded_veh_positions.latitude,
    stop_lon, stop_lat,
    ST_DISTANCE(ST_GEOGPOINT(expanded_veh_positions.longitude, expanded_veh_positions.latitude), 
    ST_GEOGPOINT(stop_lon, stop_lat)) as min_distance from expanded_veh_positions 
    JOIN min_distance_table 
    on min_distance_table.position_date = expanded_veh_positions.position_date
    and min_distance_table.trip_id = expanded_veh_positions.trip_id 
    and min_distance_table.stop_id = expanded_veh_positions.stop_id 
    and ST_DISTANCE(ST_GEOGPOINT(expanded_veh_positions.longitude, expanded_veh_positions.latitude), 
    ST_GEOGPOINT(stop_lon, stop_lat)) = min_distance_table.md
    where expanded_veh_positions.stop_sequence <> 1 and 
    expanded_veh_positions.stop_sequence <> (select max_stop_sequence from {{ ref("int_max_stop_sequence") }} as max_stop_sequence_table
    where trip_id = min_distance_table.trip_id) and EXTRACT(HOUR FROM planned_arrival_time) < 23 and EXTRACT(HOUR FROM planned_arrival_time) > 4), 

deduped as (
    select *
    from punctuality_1
    qualify row_number() over (
        partition by position_timestamp, trip_id, stop_id
        order by position_timestamp desc
    ) = 1
)

select * from deduped

