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
    p.product_id,
    p.product_name,
    p.brand,
    ct.category_name,
    pct.category_name as parent_category_name,
    rct.category_name as root_category_name
from {{ref('stg_shop__products')}} p
left join category_tree ct on p.category = ct.category_name
left join category_tree pct on ct.parent_category_id = pct.category_id
left join category_tree rct on ct.root_category_id = rct.category_id



