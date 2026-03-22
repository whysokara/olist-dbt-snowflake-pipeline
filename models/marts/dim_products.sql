with products as (
    select * from {{ ref('stg_products') }}
),

translations as (
    select * from {{ ref('stg_product_category_translations') }}
),

order_items as (
    select
        product_id,
        count(distinct order_id)        as total_orders,
        sum(price)                      as total_revenue,
        avg(price)                      as avg_price,
        sum(freight_value)              as total_freight,
        avg(freight_value)              as avg_freight
    from {{ ref('stg_order_items') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.category_name                         as category_name_portuguese,
        coalesce(t.category_name_english,
            p.category_name)                    as category_name_english,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.photos_qty,
        p.name_length,
        p.description_length,

        coalesce(oi.total_orders, 0)            as total_orders,
        coalesce(oi.total_revenue, 0)           as total_revenue,
        oi.avg_price,
        coalesce(oi.total_freight, 0)           as total_freight,
        oi.avg_freight

    from products p
    left join translations t
        on p.category_name = t.category_name_portuguese
    left join order_items oi
        on p.product_id = oi.product_id
)

select * from final