with first_order as(
    select
        customer_id,
        min(ordered_at) as first_order_at
    from {{ref('fct_orders')}}
    group by customer_id
)

select
    fo.customer_id as customer_id,
    fo.first_order_at as first_order_at,
    dc.registered_at as registered_at
from first_order fo
left join {{ref('dim_customers')}} dc on fo.customer_id = dc.customer_id
where dc.registered_at > fo.first_order_at