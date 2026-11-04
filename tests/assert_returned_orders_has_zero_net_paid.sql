select * from {{ref('fct_orders')}}
where order_status = 'returned' and net_paid_amount is distinct from 0