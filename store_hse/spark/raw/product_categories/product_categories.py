from common import create_spark, jdbc, product_categories_path

spark = create_spark("raw-product_categories")

query = """
    SELECT
        category_id,
        parent_category_id,
        category_name
    FROM public.product_categories
"""

df_product_categories = (
    spark.read
    .format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

(
    df_product_categories.write
    .format("parquet")
    .mode("overwrite")
    .save(product_categories_path)
)

print(f"Сохранено: {product_categories_path}")

spark.stop()