from pyspark.sql import functions as F
import argparse

from common import create_spark, jdbc, payments_path, sink_has_data, get_load_window

parser = argparse.ArgumentParser()

parser.add_argument(
    "--load-date",
    required = True,
    help = "Дата загрузки в формате YYYY-MM-DD"
)

args = parser.parse_args()
LOAD_DATE = args.load_date
start_date, load_date = get_load_window(LOAD_DATE)

spark = create_spark("raw-payments")

if sink_has_data(spark, payments_path):
    where = f'''
    WHERE (paid_at >= '{start_date}'::timestamp
    AND paid_at < '{load_date}'::timestamp)
    OR paid_at IS NULL
    '''
else:
    print("данных в приёмнике нет - полная загрузка таблицы")
    where = ""

query = f'''
    SELECT
        payment_id,
        order_id,
        paid_at,
        payment_status,
        amount
    FROM public.payments {where}
'''

df_payments = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

df_payments = df_payments.withColumn("paid_dt", F.to_date("paid_at"))

(
    df_payments.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("paid_dt")
    .save(payments_path)
)


print(f"Сохранено: {payments_path}")
spark.stop()