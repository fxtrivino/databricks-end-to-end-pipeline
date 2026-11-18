
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DateType,
    DecimalType,
    TimestampType
)

BRONZE_TABLE = "workspace.de_bronze.orders_raw"
SILVER_TABLE = "workspace.de_silver.orders"
INVALID_TABLE = "workspace.de_silver.invalid_orders"
GOLD_TABLE = "workspace.de_gold.fact_orders"

ORDERS_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("total_amount", DecimalType(18, 2), True),
    StructField("status", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])


def read_orders_csv(spark, path):
    return (
        spark.read
        .option("header", "true")
        .schema(ORDERS_SCHEMA)
        .csv(path)
    )


def ingest_orders_bronze(spark, df, batch_id, source_file):
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
            AND t.order_id <=> s.order_id
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("ORDERS BRONZE MERGE: PASSED")


def transform_orders_silver(spark, batch_id):
    bronze_df = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("batch_id") == batch_id)
    )

    source_rows = bronze_df.count()

    if source_rows == 0:
        raise ValueError(f"No Bronze records for batch {batch_id}")

    validated_df = bronze_df.withColumn(
        "error_reason",
        F.when(
            F.col("order_id").isNull(),
            "INVALID_ORDER_ID"
        )
        .when(
            F.col("customer_id").isNull(),
            "INVALID_CUSTOMER_ID"
        )
        .when(
            F.col("order_date").isNull(),
            "INVALID_ORDER_DATE"
        )
        .when(
            F.col("total_amount").isNull()
            | (F.col("total_amount") < 0),
            "INVALID_TOTAL_AMOUNT"
        )
        .when(
            F.col("status").isNull()
            | ~F.col("status").isin(
                "CREATED", "PAID", "CANCELLED"
            ),
            "INVALID_STATUS"
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
            f"ORDERS SILVER: Row count mismatch for batch {batch_id}. "
            f"source={source_rows}, "
            f"valid={valid_rows}, "
            f"invalid={invalid_rows}"
        )

    duplicates = (
        valid_df
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicates > 0:
        raise ValueError(
            f"Duplicate order IDs in batch: {duplicates}"
        )

    silver_to_write = valid_df.select(
        "order_id",
        "customer_id",
        "order_date",
        "total_amount",
        "status",
        "updated_at",
        "batch_id"
    )

    (
        DeltaTable.forName(spark, SILVER_TABLE)
        .alias("t")
        .merge(
            silver_to_write.alias("s"),
            "t.order_id = s.order_id"
        )
        .whenMatchedUpdate(
            condition="s.updated_at > t.updated_at",
            set={
                "customer_id": "s.customer_id",
                "order_date": "s.order_date",
                "total_amount": "s.total_amount",
                "status": "s.status",
                "updated_at": "s.updated_at",
                "batch_id": "s.batch_id",
                "processed_at": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "order_id": "s.order_id",
                "customer_id": "s.customer_id",
                "order_date": "s.order_date",
                "total_amount": "s.total_amount",
                "status": "s.status",
                "updated_at": "s.updated_at",
                "batch_id": "s.batch_id",
                "processed_at": "current_timestamp()"
            }
        )
        .execute()
    )

    if invalid_rows > 0:
        invalid_to_write = (
            invalid_df
            .select(
                "order_id",
                "customer_id",
                "order_date",
                "total_amount",
                "status",
                "updated_at",
                "batch_id",
                "error_reason"
            )
            .withColumn(
                "quarantined_at",
                F.current_timestamp()
            )
        )

        (
            DeltaTable.forName(spark, INVALID_TABLE)
            .alias("t")
            .merge(
                invalid_to_write.alias("s"),
                """
                t.batch_id = s.batch_id
                AND t.order_id <=> s.order_id
                AND t.updated_at <=> s.updated_at
                AND t.error_reason = s.error_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(
        f"ORDERS SILVER: source={source_rows}, "
        f"valid={valid_rows}, invalid={invalid_rows}"
    )

    return {
        "source_rows": source_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows
    }


def update_orders_gold(spark):
    gold_source = (
        spark.table(SILVER_TABLE)
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "total_amount",
            "status",
            "updated_at"
        )
    )

    (
        DeltaTable.forName(spark, GOLD_TABLE)
        .alias("t")
        .merge(
            gold_source.alias("s"),
            "t.order_id = s.order_id"
        )
        .whenMatchedUpdate(
            condition="s.updated_at > t.updated_at",
            set={
                "customer_id": "s.customer_id",
                "order_date": "s.order_date",
                "total_amount": "s.total_amount",
                "status": "s.status",
                "updated_at": "s.updated_at",
                "processed_at": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "order_id": "s.order_id",
                "customer_id": "s.customer_id",
                "order_date": "s.order_date",
                "total_amount": "s.total_amount",
                "status": "s.status",
                "updated_at": "s.updated_at",
                "processed_at": "current_timestamp()"
            }
        )
        .execute()
    )

    print("ORDERS GOLD MERGE: PASSED")


def validate_orders_pipeline(spark, batch_id, metrics):
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
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if silver_duplicates > 0:
        raise ValueError(
            f"Silver duplicate order IDs: {silver_duplicates}"
        )

    gold_duplicates = (
        spark.table(GOLD_TABLE)
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if gold_duplicates > 0:
        raise ValueError(
            f"Gold duplicate order IDs: {gold_duplicates}"
        )

    print("ORDERS PIPELINE VALIDATION: PASSED")


