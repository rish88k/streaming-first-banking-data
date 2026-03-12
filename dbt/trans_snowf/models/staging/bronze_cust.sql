{{ config(materialized='incremental', unique_key='customer_id') }}

with raw_source as (
    select * from {{ source('raw_data', 'RAW_CUSTOMERS') }}
),

unpacked as (
    select
        -- Metadata for CDC
        payload:op::STRING as operation_type,
        payload:ts_ms::TIMESTAMP_NTZ as event_timestamp_ms,

        -- Customer Data (Matching your DDL)
        payload:after.customer_id::UUID as customer_id,
        payload:after.first_name::STRING as first_name,
        payload:after.last_name::STRING as last_name,
        payload:after.email::STRING as email,
        payload:after.phone_number::STRING as phone_number,
        payload:after.address::STRING as address,
        payload:after.city::STRING as city,
        payload:after.country::STRING as country,
        
        -- Source Timestamps
        payload:after.created_at::TIMESTAMP_TZ as created_at,
        payload:after.updated_at::TIMESTAMP_TZ as updated_at,

        -- Audit Info
        payload:source.lsn::INT as postgres_lsn
    from raw_source
)

select * from unpacked

{% if is_incremental() %}
  where event_timestamp_ms > (select max(event_timestamp_ms) from {{ this }})
{% endif %}