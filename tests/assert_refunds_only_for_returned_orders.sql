select p.payment_id, p.order_id, p.amount, o.order_status
from {{ ref('stg_shop__payments') }} as p
left join {{ ref('stg_shop__orders') }} as o
    on p.order_id = o.order_id
where p.amount < 0 and o.order_status != "returned"