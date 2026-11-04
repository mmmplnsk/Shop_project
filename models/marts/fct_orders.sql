{{ config(
    distributed_by='order_id',
    orientation = 'column'
)}}

select
    order_id,
    customer_id,
    order_date,
    ordered_at,
    order_status,
    order_amount,
    paid_amount,
    refunded_amount,
    net_paid_amount,
    successful_payments_count,
    first_paid_at,
    items_count,
    is_confirmed
from {{ ref('int_orders_with_payments') }}
where is_test_order = false
