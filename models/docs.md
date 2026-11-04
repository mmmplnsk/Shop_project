{% docs confirmed_order %}
Подтверждённая покупка — заказ со статусом paid и хотя бы одной
успешной оплатой. Возврат оплатой не считается
{% enddocs %}

{% docs __overview__ %}
# Project: витрины данных интернет-магазина

## Структура
- **staging** - слой очищенных данных источника.
- **intermediate** - агрегация, фильтрация, джойны и иные операции с данными.
- **marts** - готовые витрины данных:
    - `fct_orders` - таблица фактов заказов
    - `fct_order_items` - таблица позиций заказов
    - `dim_customers` -  таблица измерений клиентов
    - `dim_products` - таблица измерений товаров
    - `dim_dates` - вспомогательная таблица измерений для дат

## Хранение в Greenplum

| Таблица | Распределение | Хранение |
|---|---|---|
| dim_customers | DISTRIBUTED REPLICATED | heap |
| dim_dates | DISTRIBUTED REPLICATED | heap |
| fct_orders | DISTRIBUTED BY (order_id) | ao_column |
| dim_products | DISTRIBUTED REPLICATED | heap |
| fct_order_items | DISTRIBUTED BY (order_id) | ao_column |

{% enddocs %}