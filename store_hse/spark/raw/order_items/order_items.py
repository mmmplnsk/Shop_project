from common import create_spark, jdbc, order_items_path

spark = create_spark("raw-order_items")

query = """
    SELECT
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount
    FROM public.order_items
"""

df_order_items = (
    spark.read
    .format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

(
    df_order_items.write
    .format("parquet")
    .mode("overwrite")
    .save(order_items_path)
)

print(f"Сохранено: {order_items_path}")

spark.stop()