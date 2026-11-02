with source as(
    select * from {{ source('shop', 'products') }}
)

select
    product_id,
    lower(category) as category,
    brand,
    product_name
from source