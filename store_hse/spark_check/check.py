import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType

from common import (
    create_spark,
    get_load_window,
    orders_path,
    customers_path,
    payments_path,
    stg_orders_path,
    stg_payments_path,
    int_orders_with_payments_path,
    fct_orders_path
)

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True)

args = parser.parse_args()
start_date, load_date = get_load_window(args.load_date)


#Проверка RAW-CUSTOMERS

spark = create_spark("pipeline-check")

df_customers = spark.read.format("parquet").load(customers_path)

duplicates = (
    df_customers
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

assert duplicates.count() == 0, "Найдены дубликаты по customer_id в raw-customers"

print("PASS: raw-customers")


#Проверка RAW-ORDERS

df_orders = (
    spark.read.parquet(orders_path)
    .filter(
        (F.col("order_created_dt") >= start_date) &
        (F.col("order_created_dt") < load_date)
    )
)

duplicates_orders = (
    df_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

assert duplicates_orders.count() == 0, "Найдены дубликаты по order_id в raw-orders"

print("PASS: raw-orders")



#Проверка RAW-PAYMENTS

df_payments = (
    spark.read.format("parquet").load(payments_path)
    .filter(
        ( 
            ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
            | (F.col("paid_dt").isNull())
        )
    )
)

duplicates_payments = (
    df_payments
    .groupBy("payment_id")
    .count()
    .filter(F.col("count") > 1)
)


assert duplicates_payments.count() == 0, "Найдены дубликаты по payment_id в raw-payments"

print("PASS: raw-payments")


#Проверка STG-ORDERS

df_stg_orders = (
    spark.read.parquet(stg_orders_path)
    .filter(
        (F.col("order_date") >= start_date) &
        (F.col("order_date") < load_date)
    )
)

assert len(df_stg_orders.columns) == 7, "Несоответсвие числа колонок в stg-orders"

assert isinstance(df_stg_orders.schema["order_date"].dataType, DateType), "Несоответсвие типа order_date в stg-orders"
assert isinstance(df_stg_orders.schema["is_test_order"].dataType, BooleanType), "Несоответсвие типа is_test_order в stg-orders"
assert df_orders.count() == df_stg_orders.count(), "Количество строк в raw-orders и stg-orders не совпадает"

duplicates_stg_orders = (
    df_stg_orders
    .groupBy("order_id", "order_date")
    .count()
    .filter(F.col("count") > 1)
)

assert duplicates_stg_orders.count() == 0, "Найдены дубликаты в stg-orders"

print("PASS: stg_orders")


#Проверка STG-PAYMENTS

df_stg_payments = (spark.read.format("parquet").load(stg_payments_path)
                  .filter(
                      ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
                      | F.col("paid_dt").isNull()
                  )  
            ) 

assert len(df_stg_payments.columns) == 7, "Количество колонок в stg-payments не равно 7"

assert isinstance(df_stg_payments.schema["is_refund"].dataType, BooleanType), "Тип is_refund в stg-payments не boolean"

assert df_payments.count() == df_stg_payments.count(), "Количество строк в raw-payments и stg-payments не совпадает"

stg_null_count = (
    df_stg_payments
    .filter(F.col("paid_at").isNull())
    .count()
)

if stg_null_count > 0:
    hive_path = (
        f"{stg_payments_path}/paid_dt=__HIVE_DEFAULT_PARTITION__"
    )

    hive_null_count = spark.read.parquet(hive_path).count()
    assert stg_null_count == hive_null_count, "Количество NULL-платежей в STAGING не совпадает с Hive-партицией"


print("PASS: stg_payments")


#Проверка INT-ORDER_WITH_PAYMENTS

df_int_orders_with_payments = (
    spark.read.format("parquet").load(int_orders_with_payments_path)
    .filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date)
    )
)

assert len(df_int_orders_with_payments.columns) == 16, "Количество колонок в int_orders_with_payments не равно 16"

assert df_stg_orders.count() == df_int_orders_with_payments.count(), "Количество строк в INTERMEDIATE не совпадает с STAGING orders"

duplicates_int = (
    df_int_orders_with_payments
    .groupBy("order_id", "order_date")
    .count()
    .filter(F.col("count") > 1)
)

assert duplicates_int.count() == 0, "Найдены дубликаты в int_orders_with_payments"

orders_without_payments = (
    df_stg_orders
    .join(
        df_stg_payments.select("order_id").distinct(),
        on = "order_id",
        how = "left_anti"
    )
)

df_no_payments = (
    df_int_orders_with_payments
    .join(
        orders_without_payments.select("order_id", "order_date"),
        on = ["order_id", "order_date"],
        how = "inner"
    )
)


wrong_orders = (
    df_no_payments.filter(
        (F.col("successful_payments_count") != 0) |
        (F.col("paid_amount") != 0) |
        (F.col("refunded_amount") != 0) |
        (F.col("net_paid_amount") != 0) |
        F.col("first_paid_at").isNotNull() |
        F.col("is_confirmed")
    )
)

assert wrong_orders.count() == 0, "Неверные показатели у заказов без платежей"

print("PASS: int_orders_with_payments")

#Проверка FCT_ORDERS

df_fct_orders = (
    spark.read.format("parquet").load(fct_orders_path)
    .filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date)
    )   
)

assert len(df_fct_orders.columns) == 12, "Количество колонок в fct_orders не равно 12"

df_int_without_test = df_int_orders_with_payments.filter(
    ~F.col("is_test_order")
)

assert df_int_without_test.count() == df_fct_orders.count(), "Количество строк в fct_orders не совпадает с количеством не тестовых заказов"

df_int_test_orders = df_int_orders_with_payments.filter(
    F.col("is_test_order")
)

df_temp = (
    df_fct_orders.join(
        df_int_test_orders,
        on = ["order_id","order_date"],
        how = "inner"
    )
)

assert df_temp.count() == 0, "Тестовые заказы попали в fct_orders"


duplicates_fct = (
    df_fct_orders
    .groupBy("order_id","order_date")
    .count()
    .filter(F.col("count") > 1) 
)

assert duplicates_fct.count() == 0, "Найдены дубликаты в fct_orders"

print("PASS: fct_orders")
spark.stop()