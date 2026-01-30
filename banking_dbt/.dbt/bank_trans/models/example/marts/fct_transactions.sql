with transactions as (
    select * from {{ ref('staging_bank_trans') }}
),

final as (
    select
        transaction_id,
        account_id,
        amount,
        transaction_type,
        transaction_at,
        -- Simple business logic: Is this a large transaction?
        case when amount > 5000 then true else false end as is_high_value
    from transactions
)

select * from final