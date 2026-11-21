from common import create_spark, jdbc, products_path

spark = create_spark("raw-products")

query = """
    SELECT
        product_id,
        category,
        brand,
        product_name
    FROM public.products
"""

df_products = (
    spark.read
    .format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

(
    df_products.write
    .format("parquet")
    .mode("overwrite")
    .save(products_path)
)

print(f"Сохранено: {products_path}")

spark.stop()