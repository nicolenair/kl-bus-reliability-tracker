# Plan

## punctuality

stg (shared)

intermediate query (only run once per day):
- logic: union stop_times and stop_times_mrtfeeder

intermediate query (only run once per day):
- logic: union stops and stops_mrtfeeder

intermediate query (only run once per day):
- logic: join stops and stop_times table on stop_id to get stop_times_with_coordinates table
- tables: stops, stop_times > stop_times_with_coordinates [table]

mart query (incremental update > only update rows of rtdump, stop_times_with_coordinates > actual_stop_times table that correspond to trip_ids & dates that are within the latest update batch pulled into staging): 
- logic: 
    - join rtdump table (for incremental trip ids, dates) and stop_times_with_coordinates table (for inc trip ids) on trip_id, such that for each row of the original (filtered) rtdump table, the output table will contain x rows where x = the number stops corresponding to that trip_id. then we collapse by grouping by stop_id and picking the minimum distance row with that stop_id, and we take these rows and upsert (by trip_id & stop_id & date) into the actual_stop_times table
- tables: rtdump, stop_times_with_coordinates > actual_stop_times [table]
- incremental strategy: merge on <trip_id & date>


## ghost trips

stg (shared)

mart query (incremental for new trip_ids that appeared in stg): 
- logic: only for date: in ghost_trips table
    - update to add in trips that should have happened by time (*past* trips) to ghost trips table and set by default to true
    - set is_ghost column false for the new trips that arrived
- tables: trips, actual_trips > ghost_trips [table]


note:
1. incremental updates
2. partitioning/clustering
3. backfilling, stopping the vm and restarting

docker run -it  -v ${PWD}/dbt_project:/dbt_project -v /Users/nicolenair/.dbt:/root/.dbt/ dbt-custom

bq query --nouse_legacy_sql --project_id=kl-bus-reliability-tracker 'select * from kl_bus_reliability_tracker_dataset_1.rtdump limit 10'

bq query --nouse_legacy_sql --project_id=kl-bus-reliability-tracker 'select * from kl_bus_reliability_tracker_dataset_1.stg_vehicle_positions limit 10'

bq query --nouse_legacy_sql --project_id=kl-bus-reliability-tracker 'select count(*) from kl_bus_reliability_tracker_dataset_1.rtdump'

bq query --nouse_legacy_sql --project_id=kl-bus-reliability-tracker 'select count(*) from kl_bus_reliability_tracker_dataset_1.stg_vehicle_positions limit 10'

bq query --nouse_legacy_sql --project_id=kl-bus-reliability-tracker 'select distinct position_timestamp from kl_bus_reliability_tracker_dataset_1.stg_vehicle_positions order by position_timestamp desc limit 20'

bq query --nouse_legacy_sql --project_id=kl-bus-reliability-tracker 'select distinct trip.tripId from kl_bus_reliability_tracker_dataset_1.stg_vehicle_positions order by position_timestamp desc limit 20'



NOTE:
- if possible, add receipt time


DRAFTS SQL:

```
with stop_times_with_coordinates as (SELECT  a.stop_id, a.stop_name, a.stop_code, b.stop_headsign, a.stop_lat, a.stop_lon, b.arrival_time, b.departure_time, b.trip_id, b.stop_sequence FROM `kl-bus-reliability-tracker.kl_bus_reliability_tracker_dataset_1.stops_mrtfeeder` as a JOIN `kl-bus-reliability-tracker.kl_bus_reliability_tracker_dataset_1.stop_times_mrtfeeder` as b ON a.stop_id=b.stop_id),

expanded_veh_positions as (select svp.position_timestamp as position_timestamp, svp.trip_id as trip_id, route_id, latitude, longitude, vehicle_id, vehicle_license_plate, bearing, speed, stop_times_with_coordinates.stop_id as stop_id, stop_name, stop_lat, stop_lon, arrival_time as planned_arrival_time, departure_time as planned_departure_time, stop_headsign, stop_sequence  FROM `kl-bus-reliability-tracker.kl_bus_reliability_tracker_dataset_1.stg_vehicle_positions` as svp JOIN stop_times_with_coordinates  ON svp.trip_id = stop_times_with_coordinates.trip_id), 

min_distance_table as (
select trip_id, stop_id, DATE(position_timestamp) as position_date, min(ST_DISTANCE(ST_GEOGPOINT(expanded_veh_positions.longitude, expanded_veh_positions.latitude), ST_GEOGPOINT(stop_lon, stop_lat))) as md from expanded_veh_positions group by expanded_veh_positions.route_id, expanded_veh_positions.trip_id, expanded_veh_positions.stop_id, DATE(position_timestamp)
)

select DATETIME(position_timestamp, 'Asia/Kuala_Lumpur')  as actual_arrival_time, planned_departure_time, planned_arrival_time, DATE(position_timestamp) as d, route_id, expanded_veh_positions.trip_id, expanded_veh_positions.stop_id, stop_sequence, ST_DISTANCE(ST_GEOGPOINT(expanded_veh_positions.longitude, expanded_veh_positions.latitude), ST_GEOGPOINT(stop_lon, stop_lat)) from expanded_veh_positions 
JOIN min_distance_table on min_distance_table.position_date = DATE(expanded_veh_positions.position_timestamp) and min_distance_table.trip_id = expanded_veh_positions.trip_id and min_distance_table.stop_id = expanded_veh_positions.stop_id and ST_DISTANCE(ST_GEOGPOINT(expanded_veh_positions.longitude, expanded_veh_positions.latitude), ST_GEOGPOINT(stop_lon, stop_lat)) = min_distance_table.md
where expanded_veh_positions.route_id="T805" and DATE(position_timestamp)=DATE("2026-03-23") and expanded_veh_positions.trip_id="260313020039S15" order by stop_sequence;
```


# mart queries (draft)

1
```
SELECT route_id, avg(datetime_diff(actual_arrival_time, planned_arrival_time, MINUTE)) AS lateness_minutes FROM `kl-bus-reliability-tracker.kl_bus_reliability_tracker_dataset_1.mart_punctuality` group by route_id ORDER BY lateness_minutes LIMIT 1000
```

2
```
```