# Databricks notebook source
customers_csv = """customer_id,name,email,country,updated_at
1,Ana,ana@example.com,Argentina,2026-11-10 09:00:00
2,Carlos,carlos@example.com,Chile,2026-11-10 09:10:00
3,Maria,maria@example.com,Colombia,2026-11-10 09:20:00
4,Pedro,pedro@example.com,Peru,2026-11-10 09:30:00
5,Lucia,lucia@example.com,Uruguay,2026-11-10 09:40:00
"""

volume_path = (
    "/Volumes/workspace/de_bronze/landing/customers"
)

dbutils.fs.mkdirs(volume_path)

dbutils.fs.put(
    f"{volume_path}/customers_batch_1.csv",
    customers_csv,
    overwrite=True
)

print("CUSTOMERS CSV CREATED")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType
)

customers_schema = StructType([
    StructField("customer_id", LongType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("country", StringType()),
    StructField("updated_at", TimestampType())
])

source_path = (
    "/Volumes/workspace/de_bronze/landing/"
    "customers/customers_batch_1.csv"
)

customers_df = (
    spark.read
    .option("header", "true")
    .schema(customers_schema)
    .csv(source_path)
)

display(customers_df)

# COMMAND ----------

assert customers_df.count() == 5

assert customers_df.filter(
    F.col("customer_id").isNull()
).count() == 0

print("VOLUME INGESTION TEST: PASSED")

# COMMAND ----------

source_path = (
    "/Volumes/workspace/de_bronze/landing/"
    "customers/customers_batch_4.csv"
)

csv_content = """customer_id,name,email,country,updated_at
6,Sofia,sofia@example.com,Uruguay,2026-11-18 10:00:00
10,Valentina,valentina@example.com,Argentina,2026-11-18 10:05:00
11,Lucas,invalid-email,Chile,2026-11-18 10:10:00
"""

with open(source_path, "x", encoding="utf-8") as f:
    f.write(csv_content)

print("Created:", source_path)