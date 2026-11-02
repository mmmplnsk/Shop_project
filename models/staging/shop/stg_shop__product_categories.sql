with source as(
    select * from {{ source('shop', 'product_categories') }}
)

select
    category_id,
    parent_category_id,
    lower(category_name) as category_name
from source