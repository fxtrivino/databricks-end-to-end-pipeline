# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CUSTOMERS_SILVER = "workspace.de_silver.customers"
CUSTOMERS_GOLD = "workspace.de_gold.dim_customers"

customers_source = (
    spark.table(CUSTOMERS_SILVER)
    .select(
        "customer_id",
        "name",
        "email",
        "country",
        "updated_at"
    )
    .withColumn("processed_at", F.current_timestamp())
)

(
    DeltaTable.forName(spark, CUSTOMERS_GOLD)
    .alias("t")
    .merge(
        customers_source.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("GOLD DIM_CUSTOMERS MERGE: PASSED")

# COMMAND ----------

ORDERS_SILVER = "workspace.de_silver.orders"
ORDERS_GOLD = "workspace.de_gold.fact_orders"

orders_source = (
    spark.table(ORDERS_SILVER)
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "total_amount",
        "status",
        "updated_at"
    )
    .withColumn("processed_at", F.current_timestamp())
)

(
    DeltaTable.forName(spark, ORDERS_GOLD)
    .alias("t")
    .merge(
        orders_source.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("GOLD FACT_ORDERS MERGE: PASSED")

# COMMAND ----------

PAYMENTS_SILVER = "workspace.de_silver.payments"
PAYMENTS_GOLD = "workspace.de_gold.fact_payments"

payments_source = (
    spark.table(PAYMENTS_SILVER)
    .select(
        "payment_id",
        "order_id",
        "payment_date",
        "amount",
        "payment_method",
        "status",
        "updated_at"
    )
    .withColumn("processed_at", F.current_timestamp())
)

(
    DeltaTable.forName(spark, PAYMENTS_GOLD)
    .alias("t")
    .merge(
        payments_source.alias("s"),
        "t.payment_id = s.payment_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("GOLD FACT_PAYMENTS MERGE: PASSED")