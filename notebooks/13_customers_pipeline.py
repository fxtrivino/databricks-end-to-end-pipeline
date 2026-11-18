# Databricks notebook source
# MAGIC %run ./01_batch_audit

# COMMAND ----------

dbutils.widgets.text(
    "source_file",
    "customers_batch_1.csv"
)

source_file = dbutils.widgets.get("source_file")

LANDING_PATH = (
    "/Volumes/workspace/de_bronze/landing/customers"
)

source_path = f"{LANDING_PATH}/{source_file}"

print("Source file:", source_file)
print("Source path:", source_path)

# COMMAND ----------

import hashlib

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

# Calcular SHA-256 sobre los bytes reales del archivo.
sha256 = hashlib.sha256()

with open(source_path, "rb") as file:
    for chunk in iter(lambda: file.read(1024 * 1024), b""):
        sha256.update(chunk)

input_sha256 = sha256.hexdigest()

customers_df = (
    spark.read
    .option("header", "true")
    .schema(customers_schema)
    .csv(source_path)
)

print("SHA-256:", input_sha256)
print("Rows:", customers_df.count())

display(customers_df)

# COMMAND ----------

assert customers_df.count() == 5

assert customers_df.filter(
    F.col("customer_id").isNull()
).count() == 0

assert len(input_sha256) == 64

print("PARAMETERIZED INGESTION: PASSED")

# COMMAND ----------

customers_batch_id = start_batch(
    entity_name="CUSTOMERS",
    source_file=source_file,
    input_sha256=input_sha256
)

customers_owner = acquire_attempt(
    batch_id=customers_batch_id,
    lease_minutes=60
)

print("Batch:", customers_batch_id)
print("Owner:", customers_owner)

# COMMAND ----------

assert customers_owner is not None

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == customers_batch_id)
    .first()
)

assert batch.status == "PROCESSING"
assert batch.attempt_count == 1
assert batch.active_attempt_id == customers_owner["attempt_id"]

print("PARAMETERIZED BATCH AUDIT: PASSED")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

BATCHES_TABLE = "workspace.de_control.etl_batches"
BRONZE_TABLE = "workspace.de_bronze.customers_raw"

owner_id = customers_owner["attempt_id"]

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == customers_batch_id)
    .select(
        "status",
        "active_attempt_id",
        "lease_expires_at",
        F.current_timestamp().alias("current_time")
    )
    .first()
)

assert batch is not None
assert batch.status == "PROCESSING"
assert batch.active_attempt_id == owner_id
assert batch.lease_expires_at > batch.current_time, (
    "Lease expired. Do not continue ingestion."
)

print("CUSTOMERS BATCH OWNERSHIP: PASSED")

# COMMAND ----------

customers_to_write = (
    customers_df
    .withColumn("batch_id", F.lit(customers_batch_id))
    .withColumn("source_file", F.lit(source_file))
    .withColumn("ingested_at", F.current_timestamp())
)

display(customers_to_write)

# COMMAND ----------

(
    DeltaTable.forName(spark, BRONZE_TABLE)
    .alias("t")
    .merge(
        customers_to_write.alias("s"),
        """
        t.batch_id = s.batch_id
        AND t.customer_id = s.customer_id
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("CUSTOMERS PARAMETERIZED BRONZE MERGE: PASSED")

# COMMAND ----------

bronze_batch = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("batch_id") == customers_batch_id)
)

total_rows = bronze_batch.count()

distinct_customers = (
    bronze_batch
    .select("customer_id")
    .distinct()
    .count()
)

assert total_rows == 5
assert distinct_customers == 5

print("Bronze rows:", total_rows)
print("Distinct customers:", distinct_customers)
print("CUSTOMERS PARAMETERIZED BRONZE: PASSED")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_TABLE = "workspace.de_silver.customers"
INVALID_TABLE = "workspace.de_silver.invalid_customers"

bronze_df = (
    spark.table("workspace.de_bronze.customers_raw")
    .filter(F.col("batch_id") == customers_batch_id)
)

source_rows = bronze_df.count()

assert source_rows == 5

print("Source rows:", source_rows)

# COMMAND ----------

from pyspark.sql import functions as F

validated_df = (
    bronze_df
    .withColumn(
        "error_reason",
        F.when(
            F.col("customer_id").isNull(),
            "INVALID_CUSTOMER_ID"
        )
        .when(
            F.col("name").isNull()
            | (F.trim(F.col("name")) == ""),
            "INVALID_NAME"
        )
        .when(
            F.col("email").isNull()
            | ~F.col("email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            ),
            "INVALID_EMAIL"
        )
        .when(
            F.col("country").isNull()
            | (F.trim(F.col("country")) == ""),
            "INVALID_COUNTRY"
        )
        .when(
            F.col("updated_at").isNull(),
            "INVALID_UPDATED_AT"
        )
    )
)

valid_df = (
    validated_df
    .filter(F.col("error_reason").isNull())
    .drop("error_reason")
)

invalid_df = (
    validated_df
    .filter(F.col("error_reason").isNotNull())
)

valid_rows = valid_df.count()
invalid_rows = invalid_df.count()

assert source_rows == valid_rows + invalid_rows

print("Valid:", valid_rows)
print("Invalid:", invalid_rows)

# COMMAND ----------

duplicates = (
    valid_df
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

assert duplicates.count() == 0, (
    "Duplicate customer_id found in batch"
)

print("CUSTOMERS DUPLICATE VALIDATION: PASSED")

# COMMAND ----------

silver_to_write = valid_df.select(
    "customer_id",
    "name",
    "email",
    "country",
    "updated_at",
    "batch_id"
)

(
    DeltaTable.forName(spark, SILVER_TABLE)
    .alias("t")
    .merge(
        silver_to_write.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(
        condition="s.updated_at > t.updated_at",
        set={
            "name": "s.name",
            "email": "s.email",
            "country": "s.country",
            "updated_at": "s.updated_at",
            "batch_id": "s.batch_id"
        }
    )
    .whenNotMatchedInsert(
        values={
            "customer_id": "s.customer_id",
            "name": "s.name",
            "email": "s.email",
            "country": "s.country",
            "updated_at": "s.updated_at",
            "batch_id": "s.batch_id"
        }
    )
    .execute()
)

print("CUSTOMERS PARAMETERIZED SILVER MERGE: PASSED")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

INVALID_TABLE = "workspace.de_silver.invalid_customers"

invalid_to_write = (
    invalid_df
    .select(
        "customer_id",
        "name",
        "email",
        "country",
        "updated_at",
        "batch_id",
        "error_reason"
    )
    .withColumn(
        "quarantined_at",
        F.current_timestamp()
    )
)

if invalid_rows > 0:
    (
        DeltaTable.forName(spark, INVALID_TABLE)
        .alias("t")
        .merge(
            invalid_to_write.alias("s"),
            """
            t.batch_id = s.batch_id
            AND t.customer_id <=> s.customer_id
            AND t.updated_at <=> s.updated_at
            AND t.error_reason = s.error_reason
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("CUSTOMERS QUARANTINE: PASSED")

# COMMAND ----------

from delta.tables import DeltaTable

GOLD_TABLE = "workspace.de_gold.dim_customers"

gold_source = (
    spark.table("workspace.de_silver.customers")
    .select(
        "customer_id",
        "name",
        "email",
        "country",
        "updated_at"
    )
)

(
    DeltaTable.forName(spark, GOLD_TABLE)
    .alias("t")
    .merge(
        gold_source.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(
        condition="s.updated_at > t.updated_at",
        set={
            "name": "s.name",
            "email": "s.email",
            "country": "s.country",
            "updated_at": "s.updated_at",
            "processed_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "customer_id": "s.customer_id",
            "name": "s.name",
            "email": "s.email",
            "country": "s.country",
            "updated_at": "s.updated_at",
            "processed_at": "current_timestamp()"
        }
    )
    .execute()
)

print("CUSTOMERS GOLD MERGE: PASSED")

# COMMAND ----------

from pyspark.sql import functions as F

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == customers_batch_id)
    .select(
        "status",
        "active_attempt_id",
        "lease_expires_at",
        F.current_timestamp().alias("current_time")
    )
    .first()
)

assert batch is not None
assert batch.status == "PROCESSING"
assert batch.active_attempt_id == customers_owner["attempt_id"]
assert batch.lease_expires_at > batch.current_time, (
    "Lease expired. Recovery required."
)

finish_attempt(
    batch_id=customers_batch_id,
    attempt_id=customers_owner["attempt_id"],
    attempt_number=customers_owner["attempt_number"],
    status="COMPLETED"
)

print("CUSTOMERS PARAMETERIZED PIPELINE: COMPLETED")