with raw_source as (
    select * from {{source('raw_banking', 'raw_transactions')}}
),
extract as (
    select 
        (src_json:after:transaction_id)::string as transaction_id,
        (src_json:after:account_id)::string as account_id,
        (src_json:after:amount)::decimal(15,2) as amount,
        (src_json:after:transaction_type)::string as transaction_type,
        (src_json:after:transaction_date)::timestamp_tz as transaction_at,
        ingested_at as dbt_updated_at
    from raw_source
)

select * from extract
