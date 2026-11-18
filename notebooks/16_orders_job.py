# Databricks notebook source
dbutils.widgets.text("source_file", "")
dbutils.widgets.text("lease_minutes", "60")

source_file = dbutils.widgets.get("source_file")
lease_minutes = int(dbutils.widgets.get("lease_minutes"))

if not source_file.endswith(".csv"):
    raise ValueError("Only CSV files are supported")

if "/" in source_file or "\\" in source_file:
    raise ValueError("source_file must be a filename, not a path")

if lease_minutes <= 0:
    raise ValueError("lease_minutes must be positive")

source_path = (
    "/Volumes/workspace/de_bronze/landing/orders/"
    f"{source_file}"
)

print("Source:", source_path)
print("Lease minutes:", lease_minutes)

# COMMAND ----------

#%run ./01_batch_audit
from pathlib import Path
import sys

project_root = Path.cwd().parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import src.audit as audit
import src.orders as orders

print("AUDIT MODULE IMPORT: PASSED")
print("ORDERS MODULE IMPORT: PASSED")

# COMMAND ----------

import hashlib

from delta.tables import DeltaTable
from pyspark.sql import functions as F

def calculate_file_sha256(path):
    sha256 = hashlib.sha256()

    with open(path, "rb") as file:
        while chunk := file.read(1024 * 1024):
            sha256.update(chunk)

    return sha256.hexdigest()


def check_completed_batch(entity_name, input_sha256):
    batch = (
        spark.table(audit.BATCHES_TABLE)
        .filter(
            (F.col("entity_name") == entity_name)
            & (F.col("input_sha256") == input_sha256)
        )
        .select(
            "batch_id",
            "status",
            "attempt_count"
        )
        .first()
    )

    if batch is None:
        return None

    return {
        "batch_id": batch.batch_id,
        "status": batch.status,
        "attempt_count": batch.attempt_count
    }

def decide_batch_execution(entity_name, input_sha256):
    existing_batch = check_completed_batch(
        entity_name=entity_name,
        input_sha256=input_sha256
    )

    if existing_batch is None:
        return {
            "action": "START",
            "batch_id": None,
            "reason": "NEW_BATCH"
        }

    status = existing_batch["status"]

    if status == "COMPLETED":
        return {
            "action": "SKIP",
            "batch_id": existing_batch["batch_id"],
            "reason": "ALREADY_COMPLETED"
        }

    if status in ("FAILED", "PENDING"):
        return {
            "action": "START",
            "batch_id": existing_batch["batch_id"],
            "reason": "RETRY_OR_PENDING"
        }

    if status == "PROCESSING":
        return {
            "action": "BLOCK",
            "batch_id": existing_batch["batch_id"],
            "reason": "PROCESSING_REQUIRES_LEASE_CHECK"
        }

    raise ValueError(f"Unsupported batch status: {status}")

def prepare_orders_execution(
    source_file,
    input_sha256,
    lease_minutes=60
):
    decision = decide_batch_execution(
        entity_name="ORDERS",
        input_sha256=input_sha256
    )

    if decision["action"] == "BLOCK":
        recovered = audit.recover_expired_registered_attempt(
            spark=spark,
            batch_id=decision["batch_id"]
        )

        if recovered:
            decision = decide_batch_execution(
                entity_name="ORDERS",
                input_sha256=input_sha256
            )

    if decision["action"] == "SKIP":
        return {
            "action": "SKIP",
            "batch_id": decision["batch_id"],
            "owner": None
        }

    if decision["action"] == "BLOCK":
        raise RuntimeError(
            "Batch is PROCESSING. "
            "Lease verification or recovery required."
        )

    batch_id = audit.start_batch(
        spark=spark,
        entity_name="ORDERS",
        source_file=source_file,
        input_sha256=input_sha256
    )

    owner = audit.acquire_attempt_with_retry(
        spark=spark,
        batch_id=batch_id,
        lease_minutes=lease_minutes
    )

    if owner is None:
        raise RuntimeError(
            f"Could not acquire batch {batch_id}"
        )

    return {
        "action": "PROCESS",
        "batch_id": batch_id,
        "owner": owner
    }



def run_orders_job(source_file, lease_minutes=60):

    source_path = (
        "/Volumes/workspace/de_bronze/landing/orders/"
        f"{source_file}"
    )

    input_sha256 = calculate_file_sha256(source_path)

    execution = prepare_orders_execution(
        source_file=source_file,
        input_sha256=input_sha256,
        lease_minutes=lease_minutes
    )

    if execution["action"] == "SKIP":
        print(f"SKIPPED: {execution['batch_id']}")
        return {
            "status": "SKIPPED",
            "batch_id": execution["batch_id"]
        }

    batch_id = execution["batch_id"]
    owner = execution["owner"]

    attempt_id = owner["attempt_id"]
    attempt_number = owner["attempt_number"]

    current_stage = "BRONZE"

    try:
        audit.assert_batch_ownership(
            spark, batch_id, attempt_id
        )

        # 1. Bronze
        orders_df = orders.read_orders_csv(
            spark, source_path
        )

        orders.ingest_orders_bronze(
            spark,
            orders_df,
            batch_id,
            source_file
        )

        audit.mark_stage_completed(
            spark, batch_id, attempt_id, "BRONZE"
        )

        audit.assert_batch_ownership(
            spark, batch_id, attempt_id
        )

        # 2. Silver
        current_stage = "SILVER"

        metrics = orders.transform_orders_silver(
            spark, batch_id
        )

        audit.mark_stage_completed(
            spark, batch_id, attempt_id, "SILVER"
        )

        audit.assert_batch_ownership(
            spark, batch_id, attempt_id
        )

        # 3. Gold
        current_stage = "GOLD"

        orders.update_orders_gold(spark)

        audit.mark_stage_completed(
            spark, batch_id, attempt_id, "GOLD"
        )

        audit.assert_batch_ownership(
            spark, batch_id, attempt_id
        )

        # 4. Validation
        current_stage = "VALIDATION"

        orders.validate_orders_pipeline(
            spark, batch_id, metrics
        )

        # 5. Auditoría
        current_stage = "AUDIT"

        audit.update_batch_metrics(
            spark=spark,
            batch_id=batch_id,
            attempt_id=attempt_id,
            source_rows=metrics["source_rows"],
            valid_rows=metrics["valid_rows"],
            invalid_rows=metrics["invalid_rows"]
        )

        # 6. Finalizar
        current_stage = "FINALIZATION"

        audit.finish_attempt(
            spark=spark,
            batch_id=batch_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            status="COMPLETED"
        )

        print(f"ORDERS JOB COMPLETED: {batch_id}")

        return {
            "status": "COMPLETED",
            "batch_id": batch_id,
            "metrics": metrics
        }

    except Exception as exc:

        error_message = str(exc)

        try:
            attempt_status = audit.get_attempt_status(
                spark, attempt_id
            )

            if attempt_status == "PROCESSING":

                audit.finish_attempt(
                    spark=spark,
                    batch_id=batch_id,
                    attempt_id=attempt_id,
                    attempt_number=attempt_number,
                    status="FAILED",
                    failed_stage=current_stage,
                    error_message=error_message
                )

            else:
                print(
                    f"Attempt already finalized: "
                    f"{attempt_status}"
                )

        except Exception as audit_exc:
            print(
                "Could not finalize failed attempt:",
                str(audit_exc)
            )

        raise

