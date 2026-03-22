with payments as (
    select * from {{ ref('stg_order_payments') }}
),

pivoted as (
    select
        order_id,
        count(*)                                          as payment_installments_total,
        sum(payment_value)                                as total_payment_value,
        sum(case when payment_type = 'credit_card'
            then payment_value else 0 end)                as credit_card_value,
        sum(case when payment_type = 'boleto'
            then payment_value else 0 end)                as boleto_value,
        sum(case when payment_type = 'voucher'
            then payment_value else 0 end)                as voucher_value,
        sum(case when payment_type = 'debit_card'
            then payment_value else 0 end)                as debit_card_value,
        max(payment_installments)                         as max_installments,
        count(distinct payment_type)                      as payment_methods_used
    from payments
    group by order_id
)

select * from pivoted