
{{config(materialized='table')}}



select * from {{source('raw_data', 'raw_cust')}}