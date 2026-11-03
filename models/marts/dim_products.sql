select
    product_id,
    product_name,
    brand,
    category_name,
    parent_category_name,
    root_category_name
 from {{ref('int_category_tree_with_products')}}