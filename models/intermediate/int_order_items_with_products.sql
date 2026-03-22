with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

translations as (
    select * from {{ ref('stg_product_category_translations') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

joined as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.price,
        oi.freight_value,
        oi.price + oi.freight_value  as total_item_value,
        oi.shipping_limit_date,

        p.category_name              as category_name_portuguese,
        coalesce(t.category_name_english,
            p.category_name)         as category_name_english,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.photos_qty,

        s.city                       as seller_city,
        s.state                      as seller_state,
        s.zip_code_prefix            as seller_zip_code_prefix

    from order_items oi
    left join products p
        on oi.product_id = p.product_id
    left join translations t
        on p.category_name = t.category_name_portuguese
    left join sellers s
        on oi.seller_id = s.seller_id
)

select * from joined