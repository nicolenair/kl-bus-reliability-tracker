{{ config(
    materialized='incremental', 
    unique_key=['position_timestamp', 'trip_id'],
    incremental_strategy='merge') }}
with source as (
    select * from {{ source('raw', 'rtdump') }}
),

renamed as (
    select
        -- timestamps
        timestamp_seconds(cast(timestamp as int64)) as position_timestamp,

        -- trip info
        cast(trip.tripId as string) as trip_id,
        {# cast(trip.startTime as time) as trip_start_time, #}
        {# parse_date('%Y%m%d', trip.startDate) as trip_start_date, #}
        cast(trip.routeId as string) as route_id,

        -- position
        cast(position.latitude as numeric) as latitude,
        cast(position.longitude as numeric) as longitude,
        cast(position.bearing as numeric) as bearing,
        cast(position.speed as numeric) as speed,

        -- vehicle
        cast(vehicle.id as string) as vehicle_id,
        cast(vehicle.licensePlate as string) as vehicle_license_plate

    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by position_timestamp, trip_id
        order by position_timestamp desc
    ) = 1
)

select * from deduped