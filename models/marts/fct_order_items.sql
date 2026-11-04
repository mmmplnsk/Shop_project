{{ config(
    distributed_by='order_id',
    orientation = 'column',
    materialized = 'incremental',
    unique_key = ['order_id','product_id']
)}}
with order_items as(
    select * from {{ref('int_order_items_with_customers')}}
)

select
    order_id,
    product_id,
    customer_id,
    quantity,
    unit_price,
    discount_amount,
    line_amount,
    is_confirmed
from order_items

{% if is_incremental() %}
    where not exists(
        select 1
        from {{ this }} t
        where t.order_id = order_items.order_id
            and t.product_id = order_items.product_id
    )
{% endif %}
