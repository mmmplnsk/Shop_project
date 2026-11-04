{{ config(
    distributed_by='order_id',
    orientation = 'column'
)}}

select
    order_id,
    product_id,
    customer_id,
    quantity,
    unit_price,
    discount_amount,
    line_amount,
    is_confirmed
from {{ref('int_order_items_with_customers')}}
