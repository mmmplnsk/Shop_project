from pyspark.sql import functions as F
import argparse

from common import create_spark, jdbc, events_path, sink_has_data, get_load_window

parser = argparse.ArgumentParser()

parser.add_argument(
    "--load-date",
    required = True,
    help = "Дата загрузки в формате YYYY-MM-DD"
)
args = parser.parse_args()
LOAD_DATE = args.load_date
start_date, load_date = get_load_window(LOAD_DATE)

spark = create_spark("raw-events")

if sink_has_data(spark, events_path):
    where = f'''
        WHERE event_time >= '{start_date}'::timestamp
        AND event_time < '{load_date}'::timestamp
    '''
else:
    print("данных в приёмнике нет - полная загрузка таблицы")
    where = ""

query = f'''
    SELECT
        event_id,
        customer_id,
        event_time,
        event_type,
        product_id,
        session_id
    FROM public.events {where}
'''

df_events = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

df_events = df_events.withColumn("event_dt", F.to_date("event_time"))

(
    df_events.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("event_dt")
    .save(events_path)
)

print(f"Сохранено: {events_path}")
spark.stop()