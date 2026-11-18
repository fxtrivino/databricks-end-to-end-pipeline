# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SOURCE = "workspace.de_silver.customers"
TARGET = "workspace.de_gold.dim_customers_scd2"

initial_df = (
    spark.table(SOURCE)
    .select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("customer_id").cast("string"),
                F.date_format(
                    F.col("updated_at"),
                    "yyyy-MM-dd HH:mm:ss.SSSSSS"
                )
            ),
            256
        ).alias("customer_sk"),
        "customer_id",
        "name",
        "email",
        "country",
        F.col("updated_at").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.col("updated_at").alias("source_updated_at"),
        F.current_timestamp().alias("processed_at")
    )
)

(
    DeltaTable.forName(spark, TARGET)
    .alias("t")
    .merge(
        initial_df.alias("s"),
        "t.customer_sk = s.customer_sk"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("INITIAL CUSTOMERS SCD2 LOAD: PASSED")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType
)

changes_data = [
    (
        2,
        "Carlos",
        "carlos@example.com",
        "Argentina",
        "2026-11-13 10:00:00"
    )
]

changes_schema = StructType([
    StructField("customer_id", LongType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("country", StringType()),
    StructField("updated_at", StringType())
])

changes_df = (
    spark.createDataFrame(changes_data, changes_schema)
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)

display(changes_df)

# COMMAND ----------

TARGET = "workspace.de_gold.dim_customers_scd2"

current_df = (
    spark.table(TARGET)
    .filter(F.col("is_current") == True)
    .select(
        "customer_id",
        F.col("name").alias("current_name"),
        F.col("email").alias("current_email"),
        F.col("country").alias("current_country"),
        F.col("valid_from").alias("current_valid_from")
    )
)

changed_df = (
    changes_df.alias("s")
    .join(current_df.alias("t"), "customer_id", "inner")
    .filter(
        (
            ~F.col("s.name").eqNullSafe(F.col("t.current_name"))
            | ~F.col("s.email").eqNullSafe(F.col("t.current_email"))
            | ~F.col("s.country").eqNullSafe(F.col("t.current_country"))
        )
        & (F.col("s.updated_at") > F.col("t.current_valid_from"))
    )
    .select("s.*")
)

display(changed_df)

assert changed_df.count() == 1
print("SCD2 CHANGE DETECTION: PASSED")

# COMMAND ----------

from delta.tables import DeltaTable

(
    DeltaTable.forName(spark, TARGET)
    .alias("t")
    .merge(
        changed_df.alias("s"),
        """
        t.customer_id = s.customer_id
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        condition="s.updated_at > t.valid_from",
        set={
            "valid_to": "s.updated_at",
            "is_current": "false",
            "processed_at": "current_timestamp()"
        }
    )
    .execute()
)

print("SCD2 PREVIOUS VERSION CLOSED: PASSED")

# COMMAND ----------

new_versions_df = (
    changed_df
    .select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("customer_id").cast("string"),
                F.date_format(
                    F.col("updated_at"),
                    "yyyy-MM-dd HH:mm:ss.SSSSSS"
                )
            ),
            256
        ).alias("customer_sk"),
        "customer_id",
        "name",
        "email",
        "country",
        F.col("updated_at").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.col("updated_at").alias("source_updated_at"),
        F.current_timestamp().alias("processed_at")
    )
)

(
    DeltaTable.forName(spark, TARGET)
    .alias("t")
    .merge(
        new_versions_df.alias("s"),
        "t.customer_sk = s.customer_sk"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("SCD2 NEW VERSION INSERTED: PASSED")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

TARGET = "workspace.de_gold.dim_customers_scd2"

# Usar el evento original, no changed_df.
new_version = (
    changes_df
    .select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("customer_id").cast("string"),
                F.date_format(
                    F.col("updated_at"),
                    "yyyy-MM-dd HH:mm:ss.SSSSSS"
                )
            ),
            256
        ).alias("customer_sk"),
        "customer_id",
        "name",
        "email",
        "country",
        F.col("updated_at").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.col("updated_at").alias("source_updated_at"),
        F.current_timestamp().alias("processed_at")
    )
)

(
    DeltaTable.forName(spark, TARGET)
    .alias("t")
    .merge(
        new_version.alias("s"),
        "t.customer_sk = s.customer_sk"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("SCD2 MISSING VERSION RECOVERY: EXECUTED")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

TARGET = "workspace.de_gold.dim_customers_scd2"

events_df = spark.createDataFrame(
    [
        (2, "Carlos", "carlos@example.com",
         "Uruguay", "2026-11-14 10:00:00")
    ],
    ["customer_id", "name", "email", "country", "updated_at"]
).withColumn("updated_at", F.to_timestamp("updated_at"))

# COMMAND ----------

current_df = (
    spark.table(TARGET)
    .filter(F.col("is_current") == True)
    .select(
        "customer_id",
        F.col("name").alias("old_name"),
        F.col("email").alias("old_email"),
        F.col("country").alias("old_country"),
        F.col("valid_from").alias("old_valid_from")
    )
)

changes = (
    events_df.alias("s")
    .join(current_df.alias("t"), "customer_id", "left")
    .filter(
        F.col("t.old_valid_from").isNull()
        | (
            (F.col("s.updated_at") > F.col("t.old_valid_from"))
            & (
                ~F.col("s.name").eqNullSafe(F.col("t.old_name"))
                | ~F.col("s.email").eqNullSafe(F.col("t.old_email"))
                | ~F.col("s.country").eqNullSafe(F.col("t.old_country"))
            )
        )
    )
    .select("s.*")
)

# Materializar los cambios para no volver a leer
# la dimensión después de modificarla.
changes = changes.localCheckpoint(eager=True)

assert changes.count() == 1

# Fila que coincide con la versión actual: UPDATE.
updates = changes.withColumn(
    "merge_key",
    F.col("customer_id")
)

# Fila sin clave de coincidencia: INSERT.
inserts = changes.withColumn(
    "merge_key",
    F.lit(None).cast("bigint")
)

staged = updates.unionByName(inserts)

display(staged)

# COMMAND ----------

(
    DeltaTable.forName(spark, TARGET)
    .alias("t")
    .merge(
        staged.alias("s"),
        """
        t.customer_id = s.merge_key
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        condition="s.updated_at > t.valid_from",
        set={
            "valid_to": "s.updated_at",
            "is_current": "false",
            "processed_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "customer_sk": """
                sha2(
                    concat_ws(
                        '|',
                        cast(s.customer_id AS STRING),
                        date_format(
                            s.updated_at,
                            'yyyy-MM-dd HH:mm:ss.SSSSSS'
                        )
                    ),
                    256
                )
            """,
            "customer_id": "s.customer_id",
            "name": "s.name",
            "email": "s.email",
            "country": "s.country",
            "valid_from": "s.updated_at",
            "valid_to": "CAST(NULL AS TIMESTAMP)",
            "is_current": "true",
            "source_updated_at": "s.updated_at",
            "processed_at": "current_timestamp()"
        }
    )
    .execute()
)

print("SCD2 SINGLE MERGE: EXECUTED")