# Plan

## punctuality

stg (shared)

intermediate query (only run once per day):
- logic: join stops and stop_times table on stop_id to get stop_times_with_coordinates table
- tables: stops, stop_times > stop_times_with_coordinates 

mart query (incremental update > only update rows of rtdump, stop_times_with_coordinates > actual_stop_times table that correspond to trip_ids that are within the latest update batch pulled into staging): 
- logic: 
    - join rtdump table (for inc trip ids) and stop_times_with_coordinates table (for inc trip ids) on trip_id, such that for each row of the original (filtered) rtdump table, the output table will contain x rows where x = the number stops corresponding to that trip_id. then we collapse by grouping by stop_id and picking the minimum distance row with that stop_id, and we take these rows and upsert into the actual_stop_times table
- tables: rtdump, stop_times_with_coordinates > actual_stop_times table


## ghost trips

stg (shared)

intermediate query: 
- logic: select distinct (trips, date of trip)
- tables: rtdump > actual_trips

mart query: 
- logic: join trips table and actual_trips table on trip_id (outer join to allow for nulls in actual_trips), filter for trips in the *past* and set is_ghost column based on whether actual trip_id is null
- tables: trips, actual_trips


note:
1. incremental updates
2. partitioning/clustering
3. backfilling, stopping the vm and restarting