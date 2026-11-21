import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET = "de-course"

#Raw
customers_path = f"s3a://{BUCKET}/raw/customers"
orders_path = f"s3a://{BUCKET}/raw/orders"
payments_path = f"s3a://{BUCKET}/raw/payments"
events_path = f"s3a://{BUCKET}/raw/events"
products_path = f"s3a://{BUCKET}/raw/products"
product_categories_path = f"s3a://{BUCKET}/raw/product_categories"
order_items_path = f"s3a://{BUCKET}/raw/order_items"

#Staging
stg_orders_path = f"s3a://{BUCKET}/staging/stg_orders"
stg_payments_path = f"s3a://{BUCKET}/staging/stg_payments"


#Intermediate
int_orders_with_payments_path = (
    f"s3a://{BUCKET}/intermediate/int_orders_with_payments"
)

#Marts
fct_orders_path = f"s3a://{BUCKET}/marts/fct_orders"

PG = dict(
    host="postgres",
    port=5432,
    dbname=os.getenv("POSTGRES_DB", "shop"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG["user"],
    password=PG["password"],
    driver="org.postgresql.Driver"
)


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.host", "ubuntu-vm")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def sink_has_data(spark: SparkSession, path: str) -> bool:
    try:
        return spark.read.parquet(path).limit(1).count() > 0
    except Exception:
        return False


def get_load_window(load_date_str: str):
    load_date = datetime.strptime(load_date_str, "%Y-%m-%d").date()
    start_date = load_date - timedelta(days=3)

    return start_date, load_date