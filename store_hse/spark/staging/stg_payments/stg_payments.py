from pyspark.sql import functions as F
import argparse
from common import create_spark, jdbc, payments_path, stg_payments_path, get_load_window

parser = argparse.ArgumentParser()

parser.add_argument(
    "--load-date",
    required = True,
    help = "Дата загрузки в формате YYYY-MM-DD"
)
args = parser.parse_args()
LOAD_DATE = args.load_date
start_date, load_date = get_load_window(LOAD_DATE)

spark = create_spark("stg_payments")

df_payments = (
    spark.read.format("parquet").load(payments_path)
    .filter(
        ( 
            ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
            | (F.col("paid_dt").isNull())
        )
    )
)

df_stg_payments = (
    df_payments.select(
        F.col("payment_id"),
        F.col("order_id"),
        F.col("paid_at"),
        F.col("payment_status"),
        F.col("amount"),
        ((F.col("payment_status") == "success") & (F.col("amount") < 0)).alias("is_refund")        
    )
)

df_stg_payments = df_stg_payments.withColumn("paid_dt", F.to_date("paid_at"))

(
    df_stg_payments.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("paid_dt")
    .save(stg_payments_path)
)

print(f"сохранено: {stg_payments_path}")
spark.stop()