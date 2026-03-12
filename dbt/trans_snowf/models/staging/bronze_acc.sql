{{ config(materialized='incremental', unique_key='account_id') }}

with raw_source as (
    select * from {{ source('raw_data', 'RAW_ACCOUNTS') }}
),

unpacked as (
    select
        -- Metadata for CDC
        payload:op::STRING as operation_type,
        payload:ts_ms::TIMESTAMP_NTZ as event_timestamp_ms,

        -- Account Data (Matching your DDL)
        payload:after.account_id::UUID as account_id,
        payload:after.customer_id::UUID as customer_id,
        payload:after.account_number::STRING as account_number,
        payload:after.account_type::STRING as account_type,
        
        -- Handling Decimal(15,2)
        payload:after.balance::DECIMAL(15,2) as balance,
        payload:after.currency::STRING as currency,
        payload:after.status::STRING as status,

        -- Source Timestamp
        payload:after.created_at::TIMESTAMP_TZ as created_at,

        -- Audit Info
        payload:source.lsn::INT as postgres_lsn
    from raw_source
)

select * from unpacked

{% if is_incremental() %}
  where event_timestamp_ms > (select max(event_timestamp_ms) from {{ this }})
{% endif %}