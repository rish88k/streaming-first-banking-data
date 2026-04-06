{{config(materialized='view')}}

with temp as (
    select * from {{ref('bronze_trans')}}
),

unpacked as (
    select transaction_id, amount
    case when amount_raw < 200 and amount_raw > 0 then 'rainbet'
         when amount_raw > 200 and amount_raw < 1000 then 'grocers'
         when amount_raw > 1000 then 'poker'
         else null end as Category
    from temp
),

final as (
    select count(transaction_id) as no_of_transactions,
    Category from temp
    group by Category
    order by no_of_transactions
)

select * from final

