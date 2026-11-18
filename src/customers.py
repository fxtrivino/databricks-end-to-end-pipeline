from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
)

BRONZE_TABLE = "workspace.de_bronze.customers_raw"
SILVER_TABLE = "workspace.de_silver.customers"
INVALID_TABLE = "workspace.de_silver.invalid_customers"
GOLD_TABLE = "workspace.de_gold.dim_customers"

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("updated_at", TimestampType(), True),
])

def read_customers_csv(spark, path):
    return (
        spark.read
        .option("header", "true")
        .schema(CUSTOMERS_SCHEMA)
        .csv(path)
    )


def ingest_customers_bronze(
    spark,
    df,
    batch_id,
    source_file
):
    bronze_to_write = (
        df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingested_at", F.current_timestamp())
    )

    (
        DeltaTable.forName(spark, BRONZE_TABLE)
        .alias("t")
        .merge(
            bronze_to_write.alias("s"),
            """
            t.batch_id = s.batch_id
            AND t.customer_id <=> s.customer_id
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("CUSTOMERS BRONZE MERGE: PASSED")

def transform_customers_silver(spark, batch_id):

    bronze_df = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("batch_id") == batch_id)
    )

    source_rows = bronze_df.count()

    if source_rows == 0:
        raise ValueError(
            f"No Bronze records found for batch {batch_id}"
        )

    validated_df = bronze_df.withColumn(
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

    valid_df = validated_df.filter(
        F.col("error_reason").isNull()
    )

    invalid_df = validated_df.filter(
        F.col("error_reason").isNotNull()
    )

    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()

    if source_rows != valid_rows + invalid_rows:
        raise ValueError(
            f"CUSTOMERS SILVER: Row count mismatch for batch {batch_id}. "
            f"source={source_rows}, "
            f"valid={valid_rows}, "
            f"invalid={invalid_rows}"
        )

    duplicates = (
        valid_df
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicates > 0:
        raise ValueError(
            f"Duplicate customer IDs: {duplicates}"
        )

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
                "batch_id": "s.batch_id",
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
                "batch_id": "s.batch_id",
                "processed_at": "current_timestamp()"
            }
        )
        .execute()
    )

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

    print(
        f"CUSTOMERS SILVER: source={source_rows}, "
        f"valid={valid_rows}, invalid={invalid_rows}"
    )

    return {
        "source_rows": source_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows
    }


def update_customers_gold(spark):
    gold_source = (
        spark.table(SILVER_TABLE)
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


def validate_customers_pipeline(spark, batch_id, metrics):
    bronze_count = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .count()
    )

    if bronze_count != metrics["source_rows"]:
        raise ValueError(
            f"Bronze count mismatch: "
            f"{bronze_count} != {metrics['source_rows']}"
        )

    if (
        metrics["source_rows"]
        != metrics["valid_rows"] + metrics["invalid_rows"]
    ):
        raise ValueError(
            "Source/valid/invalid counts mismatch"
        )

    invalid_count = (
        spark.table(INVALID_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .count()
    )

    if invalid_count != metrics["invalid_rows"]:
        raise ValueError(
            f"Quarantine count mismatch: "
            f"{invalid_count} != {metrics['invalid_rows']}"
        )

    silver_duplicates = (
        spark.table(SILVER_TABLE)
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if silver_duplicates > 0:
        raise ValueError(
            f"Silver duplicate customer IDs: {silver_duplicates}"
        )

    gold_duplicates = (
        spark.table(GOLD_TABLE)
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if gold_duplicates > 0:
        raise ValueError(
            f"Gold duplicate customer IDs: {gold_duplicates}"
        )

    print("CUSTOMERS PIPELINE VALIDATION: PASSED")
