-- analyses/total_revenue.sql
select sum(amount) from {{ ref('fct_transactions') }} where transaction_type = 'deposit'