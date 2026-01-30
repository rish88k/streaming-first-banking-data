{% snapshot scd_customer_status %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['status', 'credit_score'],
    )
}}

select * from {{ source('raw_banking', 'raw_customers') }}

{% endsnapshot %}