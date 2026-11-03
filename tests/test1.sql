with temp1 as(
    select
        order_id,
        sum(line_amount) as sum_line_amount
    from {{ref('fct_order_items')}}
    group by 1
),

temp2 as(
    select
        fo.order_id as order_id,
        fo.order_amount as order_amount,
        foi.sum_line_amount as sum_line_amount
    from {{ref('fct_orders')}} fo
    full outer join temp1 foi on fo.order_id = foi.order_id
)

select * from temp2
where order_amount is distinct from sum_line_amount