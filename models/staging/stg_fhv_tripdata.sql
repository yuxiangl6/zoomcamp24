{{ config(materialized='view') }}

select
    dispatching_base_num,
    Affiliated_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    SR_flag

from {{ source('staging','new_fhv_tripdata') }}