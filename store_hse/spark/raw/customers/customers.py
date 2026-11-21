from common import create_spark, jdbc, customers_path

spark = create_spark("raw-customers")

query = """
    SELECT
        customer_id,
        registered_at,
        city,
        acquisition_channel
    FROM public.customers
"""

df_customers = (
    spark.read
    .format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

(
    df_customers.write
    .format("parquet")
    .mode("overwrite")
    .save(customers_path)
)

print(f"Сохранено: {customers_path}")

spark.stop()