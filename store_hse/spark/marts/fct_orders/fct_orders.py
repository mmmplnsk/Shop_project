from pyspark.sql import functions as F
import argparse
from common import (   
    create_spark,
    int_orders_with_payments_path,
    fct_orders_path,
    get_load_window
)

parser = argparse.ArgumentParser()

parser.add_argument(
    "--load-date",
    required = True,
    help = "Дата загрузки в формате YYYY-MM-DD"
)
args = parser.parse_args()
LOAD_DATE = args.load_date
start_date, load_date = get_load_window(LOAD_DATE)

spark = create_spark("fct_orders")

df_int_orders_with_payments = (
    spark.read.format("parquet").load(int_orders_with_payments_path)
    .filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date)
           & (~F.col("is_test_order")))
)


df_fct_orders = (
    df_int_orders_with_payments.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_date"),
        F.col("ordered_at"),
        F.col("order_status"),
        F.col("order_amount"),
        F.col("paid_amount"),
        F.col("refunded_amount"),
        F.col("net_paid_amount"),
        F.col("successful_payments_count"),
        F.col("first_paid_at"),
        F.col("is_confirmed")
    )
)

(
    df_fct_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(fct_orders_path)
)

print(f"сохранено: {fct_orders_path}")
spark.stop()