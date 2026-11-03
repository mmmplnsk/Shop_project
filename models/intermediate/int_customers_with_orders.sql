with temp as (
    select
        order_id,
        customer_id,
        ordered_at,
        order_amount,
        row_number() over (
            partition by customer_id
            order by ordered_at asc, order_id asc
        ) as order_num
    from {{ref('fct_orders')}}
    where is_confirmed = true
),

first_confirmed_orders as (
    select
        order_id,
        customer_id,
        ordered_at
    from temp
    where order_num = 1
),

confirmed_orders as (
    select
        customer_id,
        count(*) as confirmed_orders_count,
        sum(order_amount) as confirmed_revenue
    from {{ref('fct_orders')}}
    where is_confirmed = true
    group by (customer_id)
)

select
    c.customer_id,
    c.registered_at,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    fco.order_id as first_confirmed_order_id,
    fco.ordered_at as first_confirmed_at,
    coalesce(co.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(co.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(co.confirmed_orders_count, 0) > 0 as is_buyer
from {{ref('stg_shop__customers')}} c
left join first_confirmed_orders fco on c.customer_id = fco.customer_id
left join confirmed_orders co on c.customer_id = co.customer_id

