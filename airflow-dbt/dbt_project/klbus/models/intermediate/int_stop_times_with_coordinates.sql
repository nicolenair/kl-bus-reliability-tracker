{{ config(materialized="table")}}
with stop_times_with_coordinates as 
(SELECT  a.stop_id, a.stop_name, 
a.stop_lat, a.stop_lon, b.arrival_time, b.departure_time, b.trip_id, 
b.stop_sequence FROM {{ ref("int_stops") }} as a 
JOIN {{ ref("int_stop_times") }} as b 
ON a.stop_id=b.stop_id)

select * from stop_times_with_coordinates