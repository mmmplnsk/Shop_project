from pyspark.sql import functions as F
import argparse
from common import create_spark, jdbc, orders_path, stg_orders_path, get_load_window

parser = argparse.ArgumentParser()

parser.add_argument(
    "--load-date",
    required = True,
    help = "Дата загрузки в формате YYYY-MM-DD"
)
args = parser.parse_args()
LOAD_DATE = args.load_date
start_date, load_date = get_load_window(LOAD_DATE)

spark = create_spark("stg_orders")

df_orders = (
    spark.read.format('parquet').load(orders_path)
    .filter((F.col("order_created_dt") >= start_date) & (F.col("order_created_dt") < load_date))
)

df_stg_orders = (
    df_orders.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_created_at").cast("timestamp").alias("ordered_at"),
        F.col("order_created_at").cast("date").alias("order_date"),
        F.col("status").alias("order_status"),
        F.col("total_amount").alias("order_amount"),
        (F.col("status") == "test").alias("is_test_order")
    )
)

(
    df_stg_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(stg_orders_path)
)

print(f"сохранено: {stg_orders_path}")
spark.stop()