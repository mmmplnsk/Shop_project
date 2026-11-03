with orders_with_payments as(
    select * from {{ref('int_orders_with_payments')}}
),

order_items as(
    select * from {{ref('stg_shop__order_items')}}
)

select
    oi.product_id,
    op.customer_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    oi.line_amount,
    op.is_confirmed
from order_items oi
left join orders_with_payments op on oi.order_id = op.order_id
where op.is_test_order = false