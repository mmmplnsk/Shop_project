from pyspark.sql import functions as F
import argparse
from common import (   
    create_spark,
    int_orders_with_payments_path,
    stg_payments_path,
    stg_orders_path,
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

spark = create_spark("int_orders_with_payments")

df_stg_orders =( spark.read.format("parquet").load(stg_orders_path)
                .filter(
                    (F.col("order_date") >= start_date) & (F.col("order_date") < load_date)
                )
            )
df_stg_payments = (spark.read.format("parquet").load(stg_payments_path)
                  .filter(
                      ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
                      | F.col("paid_dt").isNull()
                  )  
            ) 


df_order_payments = (
    df_stg_payments
    .groupBy("order_id")
    .agg(
        F.count( F.when( (F.col("payment_status") == "success") & (~F.col("is_refund")),F.lit(1))).alias("successful_payments_count"),
        F.coalesce( F.sum( F.when( (F.col("payment_status") == "success") & (~F.col("is_refund")), F.col("amount") )), F.lit(0)).alias("paid_amount"),
        F.coalesce( F.sum( F.when( F.col("is_refund"), -F.col("amount") ) ), F.lit(0)).alias("refunded_amount"),
        F.min( F.when( (F.col("payment_status") == "success") & (~F.col("is_refund")), F.col("paid_at"))).alias("first_paid_at"),
        F.count( F.when( F.col("payment_status") == "success", F.lit(1))).alias("success_operations_count"),
        F.count( F.when( F.col("payment_status") == "failed", F.lit(1))).alias("failed_operations_count"),
        F.count( F.when( F.col("payment_status") == "pending", F.lit(1))).alias("pending_operations_count")
    )
)


df_int_orders_with_payments = (
    df_stg_orders
    .join(df_order_payments, on = "order_id", how = "left")
    .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("ordered_at"),
            F.col("order_date"),
            F.col("order_status"),
            F.col("order_amount"),
            F.col("is_test_order"),
            F.coalesce(F.col("successful_payments_count"),F.lit(0)).alias("successful_payments_count"),
            F.coalesce(F.col("paid_amount"),F.lit(0)).alias("paid_amount"),
            F.coalesce(F.col("refunded_amount"),F.lit(0)).alias("refunded_amount"),
            (F.coalesce(F.col("paid_amount"),F.lit(0)) - F.coalesce(F.col("refunded_amount"),F.lit(0))).alias("net_paid_amount"),
            F.col("first_paid_at"),
            ((F.col("order_status") == "paid") & (F.coalesce(F.col("successful_payments_count"),F.lit(0)) > 0)).alias("is_confirmed"),
            F.coalesce(F.col("success_operations_count"), F.lit(0)).alias("success_operations_count"),
            F.coalesce(F.col("failed_operations_count"), F.lit(0)).alias("failed_operations_count"),
            F.coalesce(F.col("pending_operations_count"), F.lit(0)).alias("pending_operations_count")
    )
)

df_int_orders_with_payments.write.format("parquet").mode("overwrite").partitionBy("order_date").save(int_orders_with_payments_path)

print(f"сохранено: {int_orders_with_payments_path}")
spark.stop()