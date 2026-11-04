with temp1 as(
select
    order_id,
    count(case when payment_status = 'success' and amount > 0 then 1 else null end) > 0 as has_successful_payment
from {{source('shop', 'payments')}}
group by order_id
),

raw_confirmed_orders as(
    select
        o.order_id as order_id,
        o.total_amount as order_amount,
        coalesce(temp1.has_successful_payment, false) as has_successful_payment
    from {{source('shop','orders')}} o
    left join temp1 on o.order_id = temp1.order_id
    where coalesce(temp1.has_successful_payment, false) = true
    and o.status = 'paid'
),

fct_confirmed_orders as(
    select
        fo.order_id as order_id,
        fo.order_amount as order_amount
    from {{ref('fct_orders')}} fo
    where is_confirmed = true
),

temp2 as(
    select
        fco.order_id as order_id_fct_orders,
        rco.order_id as order_id_raw,
        coalesce(fco.order_amount,0) as order_amount_fct_orders,
        coalesce(rco.order_amount,0) as order_amount_raw
    from fct_confirmed_orders fco
    full outer join raw_confirmed_orders rco ON fco.order_id = rco.order_id
)
    
select * from temp2
where order_amount_fct_orders != order_amount_raw

