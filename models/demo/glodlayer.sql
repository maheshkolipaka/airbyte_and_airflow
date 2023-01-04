{{ config(materialized='table',schema ='Gold') }}

with groupby(PRODUCTTYPE, VALUE, CURRENCY)
as(
    select producttype, sum(value), currency from {{ ref('silverdata') }}
    group by producttype, currency
    order by producttype,currency
)
select * from groupby   