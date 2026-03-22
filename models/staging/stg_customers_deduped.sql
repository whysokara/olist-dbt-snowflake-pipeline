with source as (
    select * from {{ ref('stg_customers') }}
),

deduped as (
    select distinct
        customer_unique_id,
        max(zip_code_prefix)    as zip_code_prefix,
        max(city)               as city,
        max(state)              as state
    from source
    group by customer_unique_id
)

select * from deduped