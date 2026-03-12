{{ config(
    materialized='incremental', 
    unique_key='transaction_id'
) }}

with raw_source as (
    -- This assumes your Snowflake landing table is named 'raw_acc_transactions'
    select * from {{ source('raw_data', 'RAW_TRANSACTIONS') }}
),

unpacked as (
    select
        -- Metadata for tracking the Change Data Capture (CDC) stream
        payload:op::STRING as operation_type,
        payload:ts_ms::TIMESTAMP_NTZ as event_timestamp_ms,

        -- Primary & Foreign Keys (matching your UUID requirement)
        payload:after.transaction_id::UUID as transaction_id,
        payload:after.account_id::UUID as account_id,

        -- Transaction Details
        payload:after.transaction_type::STRING as transaction_type,
        payload:after.amount::DECIMAL(15,2) as amount,
        payload:after.description::STRING as description,
        payload:after.merchant_name::STRING as merchant_name,
        
        -- Timestamp (matching your Postgres 'transaction_date')
        payload:after.transaction_date::TIMESTAMP_TZ as transaction_date,

        -- Audit Info from the source database
        payload:source.lsn::INT as postgres_lsn
    from raw_source
)

select * from unpacked

{% if is_incremental() %}
  -- This ensures we only 'upsert' records that have changed since the last run
  where event_timestamp_ms > (select max(event_timestamp_ms) from {{ this }})
{% endif %}