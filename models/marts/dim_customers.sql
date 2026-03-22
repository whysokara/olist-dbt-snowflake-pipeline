with customers as (
    select distinct
        customer_unique_id,
        -- take any one zip/city/state per unique customer
        max(zip_code_prefix)    as zip_code_prefix,
        max(city)               as city,
        max(state)              as state
    from {{ ref('stg_customers') }}
    group by customer_unique_id
),

orders as (
    select
        customer_unique_id,
        count(distinct order_id)            as total_orders,
        min(purchased_at)                   as first_order_at,
        max(purchased_at)                   as last_order_at,
        sum(total_order_value)              as lifetime_value,
        avg(review_score)                   as avg_review_score,
        sum(case when is_delivered_on_time
            then 1 else 0 end)              as on_time_deliveries,
        count(distinct order_id)            as total_deliveries
    from {{ ref('fct_orders') }}
    where order_status = 'delivered'
    group by customer_unique_id
),

final as (
    select
        c.customer_unique_id,
        c.zip_code_prefix,
        c.city,
        c.state,

        coalesce(o.total_orders, 0)         as total_orders,
        o.first_order_at,
        o.last_order_at,
        coalesce(o.lifetime_value, 0)       as lifetime_value,
        o.avg_review_score,
        coalesce(o.on_time_deliveries, 0)   as on_time_deliveries,
        coalesce(o.total_deliveries, 0)     as total_deliveries,

        case
            when o.total_orders is null then 'no_orders'
            when o.total_orders = 1     then 'one_time'
            when o.total_orders <= 3    then 'repeat'
            else 'loyal'
        end                                 as customer_segment,

        datediff('day',
            o.first_order_at,
            o.last_order_at
        )                                   as customer_lifespan_days

    from customers c
    left join orders o
        on c.customer_unique_id = o.customer_unique_id
)

select * from final