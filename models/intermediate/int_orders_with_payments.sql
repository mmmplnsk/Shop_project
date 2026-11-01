-- Задание 4. Одна строка — один заказ, к нему пристыкованы итоги по платежам.
-- Сначала сверните платежи до заказа (CTE order_payments), потом присоедините их к заказам.
with orders as (
    select * from {{ ref('stg_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_shop__payments') }}
),

order_payments as (
    select
        order_id,
        count(case when payment_status = 'success' and is_refund = false then 1 end) as successful_payments_count,  -- успешные оплаты без возвратов
        sum(case when payment_status = 'success' and is_refund = false then amount else 0 end) as paid_amount,                -- их сумма; 0, если их нет
        sum(case when is_refund = true then amount else 0 end) as refunded_amount,            -- сумма возвратов положительным числом; 0, если их нет
        min(case when payment_status = 'success' and is_refund = false then paid_at end)  as first_paid_at               -- время первой успешной оплаты
    from payments
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.order_date,
    o.order_status,
    o.order_amount,
    o.is_test_order,
    p.successful_payments_count  as successful_payments_count,   -- 0 вместо null
    p.paid_amount as paid_amount,
    p.refunded_amount    as refunded_amount,
    p.paid_amount - p.refunded_amount    as net_paid_amount,             -- оплачено минус возвращено
    p.first_paid_at,
    (o.order_status = 'paid' and p.successful_payments_count > 0) as is_confirmed                 -- статус paid и хотя бы одна успешная оплата
from orders as o
left join order_payments as p
    on p.order_id = o.order_id
