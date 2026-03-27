{{ config(materialized="table")}}
with max_stop_sequence_table as (
    select trip_id,  
    max(stop_sequence) as max_stop_sequence
    from {{ ref("int_stop_times_with_coordinates") }}
    group by 
    trip_id)

select * from max_stop_sequence_table