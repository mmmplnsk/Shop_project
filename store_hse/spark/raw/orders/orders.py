from pyspark.sql import functions as F
import argparse

from common import create_spark, jdbc, orders_path, sink_has_data, get_load_window

parser = argparse.ArgumentParser()

parser.add_argument(
    "--load-date",
    required = True,
    help = "Дата загрузки в формате YYYY-MM-DD"
)
args = parser.parse_args()
LOAD_DATE = args.load_date
start_date, load_date = get_load_window(LOAD_DATE)



spark = create_spark("raw-orders")

if sink_has_data(spark, orders_path):
    where = f"""
        WHERE order_created_at >= '{start_date}'::timestamp
            AND order_created_at < '{load_date}'::timestamp
    """
else:
    print("данных в приёмнике нет - полная загрузка таблицы")
    where = ""

query = f"""
    SELECT 
        order_id,
        customer_id,
        order_created_at,
        status,
        total_amount
    FROM public.orders {where}
"""

df_orders = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

df_orders = df_orders.withColumn("order_created_dt", F.to_date("order_created_at"))

(
    df_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_created_dt")
    .save(orders_path)
)

print(f"Сохранено: {orders_path}")

spark.stop()