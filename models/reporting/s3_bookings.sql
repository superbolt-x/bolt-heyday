{{ config (
    alias = target.database + '_s3_bookings',
    materialized='incremental',
    unique_key='unique_key'
)}}

WITH last_record_date as (select 
appointment_id, max(_fivetran_synced) as last_synced_date
from {{ source('s3_raw','bookings_raw') }}
group by appointment_id)

, final_data as (select 
    bookings_raw.appointment_id,
    created_date,
    location_name,
    client_type,
    updated_date,
    status,
    appointment_date,
    "count" as appointments,
    bookings_raw.appointment_id as unique_key
from last_record_date
left join {{ source('s3_raw','bookings_raw') }} 
on bookings_raw._fivetran_synced = last_record_date.last_synced_date
and bookings_raw.appointment_id = last_record_date.appointment_id)

select * from final_data 
