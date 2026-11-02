with recursive

stg_product_categories as(
    select * from {{ ref('stg_shop__product_categories')}}
),

 category_tree as(

    select
        category_id,
        category_name,
        parent_category_id,
        category_id                 as root_category_id,
        0                           as depth,
        array[category_id]          as id_path
    from stg_product_categories
    where parent_category_id is null

    union all

    select
        c.category_id,
        c.category_name,
        c.parent_category_id,
        ct.root_category_id,
        ct.depth + 1                as depth,
        ct.id_path || c.category_id as id_path
    from  stg_product_categories c
    inner join category_tree ct
        on c.parent_category_id = ct.category_id
    where ct.depth < 8
        and not c.category_id = any(ct.id_path)
)

select
    category_id,
    category_name,
    parent_category_id,
    root_category_id
from
    category_tree