from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
    DecimalType
)

BRONZE_TABLE = "workspace.de_bronze.payments_raw"
SILVER_TABLE = "workspace.de_silver.payments"
INVALID_TABLE = "workspace.de_silver.invalid_payments"
GOLD_TABLE = "workspace.de_gold.fact_payments"

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("payment_date", TimestampType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])


def read_payments_csv(spark, path):
    return (
        spark.read
        .option("header", "true")
        .schema(PAYMENTS_SCHEMA)
        .csv(path)
    )


def ingest_payments_bronze(
    spark,
    df,
    batch_id,
    source_file
):
    bronze_df = (
        df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingested_at", F.current_timestamp())
    )

    bronze_delta = DeltaTable.forName(
        spark,
        BRONZE_TABLE
    )

    (
        bronze_delta.alias("t")
        .merge(
            bronze_df.alias("s"),
            """
            t.batch_id = s.batch_id
            AND t.payment_id <=> s.payment_id
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("PAYMENTS BRONZE MERGE: PASSED")


def transform_payments_silver(spark, batch_id):

    bronze_df = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("batch_id") == batch_id)
    )

    source_rows = bronze_df.count()

    if source_rows == 0:
        raise ValueError(
            f"No Bronze records found for batch {batch_id}"
        )

    # 1. Validación de datos
    validated_df = bronze_df.withColumn(
        "error_reason",
        F.when(
            F.col("payment_id").isNull(),
            "INVALID_PAYMENT_ID"
        )
        .when(
            F.col("order_id").isNull(),
            "INVALID_ORDER_ID"
        )
        .when(
            F.col("payment_date").isNull(),
            "INVALID_PAYMENT_DATE"
        )
        .when(
            F.col("amount").isNull()
            | (F.col("amount") <= 0),
            "INVALID_AMOUNT"
        )
        .when(
            F.col("payment_method").isNull()
            | ~F.col("payment_method").isin(
                "CREDIT_CARD",
                "DEBIT_CARD",
                "TRANSFER"
            ),
            "INVALID_PAYMENT_METHOD"
        )
        .when(
            F.col("status").isNull()
            | ~F.col("status").isin(
                "PENDING",
                "COMPLETED",
                "FAILED",
                "REFUNDED"
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
            "Source/valid/invalid counts mismatch"
        )

    # 2. Detectar payment_id duplicados en el batch
    duplicate_count = (
        valid_df
        .groupBy("payment_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate payment IDs in batch: {duplicate_count}"
        )

    # 3. MERGE de pagos válidos hacia Silver
    silver_df = (
        valid_df
        .select(
            "payment_id",
            "order_id",
            "payment_date",
            "amount",
            "payment_method",
            "status",
            "updated_at",
            "batch_id"
        )
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )
    )

    silver_delta = DeltaTable.forName(
        spark,
        SILVER_TABLE
    )

    (
        silver_delta.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.payment_id = s.payment_id"
        )
        .whenMatchedUpdateAll(
            condition="s.updated_at > t.updated_at"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 4. MERGE de pagos inválidos hacia Quarantine
    quarantine_df = (
        invalid_df
        .select(
            "payment_id",
            "order_id",
            "payment_date",
            "amount",
            "payment_method",
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

    invalid_delta = DeltaTable.forName(
        spark,
        INVALID_TABLE
    )

    (
        invalid_delta.alias("t")
        .merge(
            quarantine_df.alias("s"),
            """
            t.batch_id = s.batch_id
            AND t.payment_id <=> s.payment_id
            AND t.updated_at <=> s.updated_at
            AND t.error_reason = s.error_reason
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"PAYMENTS SILVER: "
        f"source={source_rows}, "
        f"valid={valid_rows}, "
        f"invalid={invalid_rows}"
    )

    return {
        "source_rows": source_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows
    }


def update_payments_gold(spark):

    silver_df = (
        spark.table(SILVER_TABLE)
        .select(
            "payment_id",
            "order_id",
            "payment_date",
            "amount",
            "payment_method",
            "status",
            "updated_at"
        )
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )
    )

    gold_delta = DeltaTable.forName(
        spark,
        GOLD_TABLE
    )

    (
        gold_delta.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.payment_id = s.payment_id"
        )
        .whenMatchedUpdateAll(
            condition="s.updated_at > t.updated_at"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("PAYMENTS GOLD MERGE: PASSED")


def validate_payments_pipeline(spark, batch_id, metrics):

    # 1. Validar Bronze
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

    # 2. Validar métricas
    if (
        metrics["source_rows"]
        != metrics["valid_rows"] + metrics["invalid_rows"]
    ):
        raise ValueError(
            "Source/valid/invalid counts mismatch"
        )

    # 3. Validar Quarantine
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

    # 4. Detectar duplicados en Silver
    silver_duplicates = (
        spark.table(SILVER_TABLE)
        .groupBy("payment_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if silver_duplicates > 0:
        raise ValueError(
            f"Silver duplicate payment IDs: "
            f"{silver_duplicates}"
        )

    # 5. Detectar duplicados en Gold
    gold_duplicates = (
        spark.table(GOLD_TABLE)
        .groupBy("payment_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if gold_duplicates > 0:
        raise ValueError(
            f"Gold duplicate payment IDs: "
            f"{gold_duplicates}"
        )

    print("PAYMENTS PIPELINE VALIDATION: PASSED")

