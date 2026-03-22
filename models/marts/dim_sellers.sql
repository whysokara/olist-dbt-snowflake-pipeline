with sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select
        seller_id,
        count(distinct order_id)        as total_orders,
        count(*)                        as total_items_sold,
        sum(price)                      as total_revenue,
        avg(price)                      as avg_item_price,
        sum(freight_value)              as total_freight,
        count(distinct product_id)      as unique_products_sold
    from {{ ref('stg_order_items') }}
    group by seller_id
),

reviews as (
    select
        oi.seller_id,
        avg(r.review_score)             as avg_review_score,
        count(r.review_id)              as total_reviews
    from {{ ref('stg_order_reviews') }} r
    left join {{ ref('stg_order_items') }} oi
        on r.order_id = oi.order_id
    group by oi.seller_id
),

final as (
    select
        s.seller_id,
        s.zip_code_prefix,
        s.city                                  as seller_city,
        s.state                                 as seller_state,

        coalesce(oi.total_orders, 0)            as total_orders,
        coalesce(oi.total_items_sold, 0)        as total_items_sold,
        coalesce(oi.total_revenue, 0)           as total_revenue,
        oi.avg_item_price,
        coalesce(oi.total_freight, 0)           as total_freight,
        coalesce(oi.unique_products_sold, 0)    as unique_products_sold,

        r.avg_review_score,
        coalesce(r.total_reviews, 0)            as total_reviews,

        case
            when oi.total_revenue >= 100000 then 'platinum'
            when oi.total_revenue >= 50000  then 'gold'
            when oi.total_revenue >= 10000  then 'silver'
            else 'bronze'
        end                                     as seller_tier

    from sellers s
    left join order_items oi
        on s.seller_id = oi.seller_id
    left join reviews r
        on s.seller_id = r.seller_id
)

select * from final