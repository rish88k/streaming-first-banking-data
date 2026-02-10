{{config(materialized='view')}}


with temp as (
    select * from {{ref('bronze_cust')}}
),

unpacked as (
    select count(cust_id) as customers, country
    from temp 
    group by country
    order by customers desc
)