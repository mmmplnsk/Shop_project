|relname|distribution|storage|
|-------|------------|-------|
|dim_customers|DISTRIBUTED REPLICATED|heap|
|dim_dates|DISTRIBUTED REPLICATED|heap|
|fct_orders|DISTRIBUTED BY (order_id)|ao_column|
|dim_products|DISTRIBUTED REPLICATED|heap|
|fct_order_items|DISTRIBUTED BY (order_id)|ao_column|
