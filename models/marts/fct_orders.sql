{{config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
)}}

with orders as (
    select * from {{ ref('int_orders_with_customers') }}
    {% if is_incremental() %}
    where purchased_at >= (select max(purchased_at) from {{ this }})
    {% endif %}
),

payments as (
    select * from {{ ref('int_order_payments_pivoted') }}
),

order_items as (
    select
        order_id,
        count(*)                    as total_items,
        sum(price)                  as total_items_value,
        sum(freight_value)          as total_freight_value,
        sum(total_item_value)       as total_order_value
    from {{ ref('int_order_items_with_products') }}
    group by order_id
),

reviews as (
    select
        order_id,
        -- take the most recent review if multiple exist
        max(review_score)           as review_score,
        max(comment_message)        as review_comment
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.customer_unique_id,
        o.customer_city,
        o.customer_state,
        o.status                            as order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_to_carrier_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,
        o.actual_delivery_days,
        o.estimated_delivery_days,
        o.is_delivered_on_time,

        oi.total_items,
        oi.total_items_value,
        oi.total_freight_value,
        oi.total_order_value,

        p.total_payment_value,
        p.payment_methods_used,
        p.max_installments,
        p.credit_card_value,
        p.boleto_value,
        p.voucher_value,
        p.debit_card_value,

        r.review_score,
        r.review_comment           as review_comment,

        date_trunc('month', o.purchased_at) as order_month,
        date_trunc('year', o.purchased_at)  as order_year

    from orders o
    left join payments p
        on o.order_id = p.order_id
    left join order_items oi
        on o.order_id = oi.order_id
    left join reviews r
        on o.order_id = r.order_id
)

select * from final