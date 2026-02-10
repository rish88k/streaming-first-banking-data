
{{config(matarialized='incremental') }}



with raw_source as (
    select * from {{ source('raw_data', 'raw_trans') }}
),

unpacked as (
    select
        -- 1. Extract Metadata
        payload:op::STRING as operation_type, -- 'c' for create, 'u' for update
        payload:ts_ms::TIMESTAMP_NTZ as event_timestamp_ms,
        
        -- 2. Extract 'After' State (The actual row data)
        payload:after.transaction_id::STRING as transaction_id,
        payload:after.account_id::STRING as account_id,
        payload:after.transaction_type::STRING as transaction_type,
        
        -- Special Handling: Base64 Decimals (Debezium often encodes decimals as bytes)
        -- Note: If Snowflake doesn't auto-decode 'ALVw', you may need TO_DECIMAL(base64_decode_string(...))
        payload:after.amount::STRING as amount_raw, 
        
        payload:after.transaction_date::TIMESTAMP_TZ as transaction_at,
        payload:after.description::STRING as description,
        payload:after.merchant_name::STRING as merchant_name,
        
        -- 3. Extract Source Info (Useful for auditing)
        payload:source.db::STRING as source_db,
        payload:source.table::STRING as source_table,
        payload:source.lsn::INT as postgres_lsn

    from raw_source
)

select * from unpacked