with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.status,
        o.purchased_at,
        o.approved_at,
        o.delivered_to_carrier_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,

        c.customer_unique_id,
        c.zip_code_prefix            as customer_zip_code_prefix,
        c.city                       as customer_city,
        c.state                      as customer_state,

        datediff('day',
            o.purchased_at,
            o.delivered_to_customer_at
        )                            as actual_delivery_days,

        datediff('day',
            o.purchased_at,
            o.estimated_delivery_at
        )                            as estimated_delivery_days,

        case
            when o.delivered_to_customer_at <= o.estimated_delivery_at
                then true
            else false
        end                          as is_delivered_on_time

    from orders o
    left join customers c
        on o.customer_id = c.customer_id
)

select * from joined