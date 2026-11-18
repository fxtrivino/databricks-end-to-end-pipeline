import hashlib
import uuid
import random
import time

from pyspark.sql import functions as F


BATCHES_TABLE = "workspace.de_control.etl_batches"
ATTEMPTS_TABLE = "workspace.de_control.etl_batch_attempts"


def start_batch(spark, entity_name, source_file, input_sha256):
    entity_name = entity_name.upper()

    batch_id = hashlib.sha256(
        f"{entity_name}:{input_sha256}".encode("utf-8")
    ).hexdigest()

    existing = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .limit(1)
        .collect()
    )

    if existing:
        print(f"Batch already exists: {batch_id}")
        return batch_id

    spark.sql(
        f"""
        INSERT INTO {BATCHES_TABLE} (
            batch_id,
            entity_name,
            source_file,
            input_sha256,
            status,
            attempt_count,
            created_at,
            updated_at
        )
        VALUES (
            :batch_id,
            :entity_name,
            :source_file,
            :input_sha256,
            'PENDING',
            0,
            current_timestamp(),
            current_timestamp()
        )
        """,
        args={
            "batch_id": batch_id,
            "entity_name": entity_name,
            "source_file": source_file,
            "input_sha256": input_sha256,
        },
    )

    print(f"Batch created: {batch_id}")
    return batch_id


def acquire_attempt(spark, batch_id, lease_minutes=30):
    if lease_minutes <= 0:
        raise ValueError("lease_minutes must be positive")

    attempt_id = str(uuid.uuid4())

    # 1. Reservar el batch si está disponible.
    result = spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'PROCESSING',
            attempt_count = attempt_count + 1,
            active_attempt_id = :attempt_id,
            heartbeat_at = current_timestamp(),
            lease_expires_at =
                current_timestamp()
                + INTERVAL {int(lease_minutes)} MINUTES,
            updated_at = current_timestamp(),
            error_message = NULL
        WHERE batch_id = :batch_id
          AND status IN ('PENDING', 'FAILED')
          AND active_attempt_id IS NULL
    """, args={
        "batch_id": batch_id,
        "attempt_id": attempt_id
    })

    if result.first().num_affected_rows != 1:
        print("Attempt acquisition rejected")
        return None

    # 2. Comprobar ownership y obtener el número de intento.
    batch = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if batch is None or batch.active_attempt_id != attempt_id:
        raise RuntimeError("Lost ownership after acquisition")

    attempt_number = batch.attempt_count

    # 3. Registrar el intento.
    spark.sql(f"""
        INSERT INTO {ATTEMPTS_TABLE} (
            attempt_id,
            batch_id,
            attempt_number,
            status,
            started_at,
            created_at
        )
        VALUES (
            :attempt_id,
            :batch_id,
            :attempt_number,
            'PROCESSING',
            current_timestamp(),
            current_timestamp()
        )
    """, args={
        "attempt_id": attempt_id,
        "batch_id": batch_id,
        "attempt_number": attempt_number
    })

    print(
        f"Attempt acquired: {attempt_id}, "
        f"number={attempt_number}"
    )

    return {
        "attempt_id": attempt_id,
        "attempt_number": attempt_number
    }


def finish_attempt(
    spark,
    batch_id,
    attempt_id,
    attempt_number,
    status,
    failed_stage=None,
    error_message=None
):
    if status not in ("COMPLETED", "FAILED"):
        raise ValueError("Invalid final status")

    if status == "COMPLETED":
        failed_stage = None
        error_message = None

    # 1. Finalizar el batch y liberar ownership.
    result = spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = :status,
            active_attempt_id = NULL,
            lease_expires_at = NULL,
            heartbeat_at = NULL,
            updated_at = current_timestamp(),
            error_message = :error_message
        WHERE batch_id = :batch_id
          AND status = 'PROCESSING'
          AND active_attempt_id = :attempt_id
          AND attempt_count = :attempt_number
          AND lease_expires_at > current_timestamp()
    """, args={
        "status": status,
        "error_message": error_message,
        "batch_id": batch_id,
        "attempt_id": attempt_id,
        "attempt_number": attempt_number
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            "Attempt cannot finalize: ownership lost "
            "or lease expired"
        )

    # 2. Finalizar el registro del intento.
    result = spark.sql(f"""
        UPDATE {ATTEMPTS_TABLE}
        SET
            status = :status,
            finished_at = current_timestamp(),
            duration_seconds = timestampdiff(
                SECOND,
                started_at,
                current_timestamp()
            ),
            failed_stage = :failed_stage,
            error_message = :error_message
        WHERE attempt_id = :attempt_id
          AND batch_id = :batch_id
          AND attempt_number = :attempt_number
          AND status = 'PROCESSING'
    """, args={
        "status": status,
        "failed_stage": failed_stage,
        "error_message": error_message,
        "attempt_id": attempt_id,
        "batch_id": batch_id,
        "attempt_number": attempt_number
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            "Batch finalized but attempt audit update failed; "
            "reconciliation required"
        )

    print(f"Attempt finalized: {attempt_id}, status={status}")
    return True

def renew_batch_lease(spark, batch_id, attempt_id, lease_minutes=30):
    if lease_minutes <= 0:
        raise ValueError("lease_minutes must be positive")

    result = spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            heartbeat_at = current_timestamp(),
            lease_expires_at =
                current_timestamp()
                + INTERVAL {int(lease_minutes)} MINUTES,
            updated_at = current_timestamp()
        WHERE batch_id = :batch_id
          AND active_attempt_id = :attempt_id
          AND status = 'PROCESSING'
          AND lease_expires_at > current_timestamp()
    """, args={
        "batch_id": batch_id,
        "attempt_id": attempt_id
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            "Lease renewal rejected: ownership lost or lease expired"
        )

    batch = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if (
        batch is None
        or batch.active_attempt_id != attempt_id
        or batch.status != "PROCESSING"
    ):
        raise RuntimeError("Lease renewal could not be confirmed")

    return batch.lease_expires_at


def recover_expired_registered_attempt(spark, batch_id):
    batch = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if batch is None:
        raise ValueError("Batch not found")

    if batch.status != "PROCESSING":
        return False

    if batch.active_attempt_id is None:
        return False

    attempt_id = batch.active_attempt_id
    attempt_number = batch.attempt_count

    attempt = (
        spark.table(ATTEMPTS_TABLE)
        .filter(
            (F.col("attempt_id") == attempt_id)
            & (F.col("batch_id") == batch_id)
            & (F.col("attempt_number") == attempt_number)
        )
        .first()
    )

    if attempt is None or attempt.status != "PROCESSING":
        raise RuntimeError(
            "Attempt audit inconsistent; reconciliation required"
        )

    # Recuperar únicamente si el lease expiró
    # y el intento todavía es el propietario.
    result = spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'FAILED',
            active_attempt_id = NULL,
            lease_expires_at = NULL,
            heartbeat_at = NULL,
            updated_at = current_timestamp(),
            error_message = 'Expired lease recovered'
        WHERE batch_id = :batch_id
          AND status = 'PROCESSING'
          AND active_attempt_id = :attempt_id
          AND attempt_count = :attempt_number
          AND lease_expires_at < current_timestamp()
    """, args={
        "batch_id": batch_id,
        "attempt_id": attempt_id,
        "attempt_number": attempt_number
    })

    if result.first().num_affected_rows != 1:
        return False

    # Registrar el fallo del intento.
    result = spark.sql(f"""
        UPDATE {ATTEMPTS_TABLE}
        SET
            status = 'FAILED',
            finished_at = current_timestamp(),
            duration_seconds = timestampdiff(
                SECOND,
                started_at,
                current_timestamp()
            ),
            failed_stage = 'LEASE',
            error_message = 'Lease expired during processing'
        WHERE attempt_id = :attempt_id
          AND batch_id = :batch_id
          AND attempt_number = :attempt_number
          AND status = 'PROCESSING'
    """, args={
        "attempt_id": attempt_id,
        "batch_id": batch_id,
        "attempt_number": attempt_number
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            "Batch recovered but attempt update failed; "
            "reconciliation required"
        )

    print(f"Expired attempt recovered: {attempt_id}")
    return True

def acquire_attempt_with_retry(
    spark,
    batch_id,
    lease_minutes=30,
    max_retries=3
):
    if max_retries < 0:
        raise ValueError("max_retries cannot be negative")

    concurrency_errors = (
        "DELTA_CONCURRENT_APPEND",
        "DELTA_CONCURRENT_MODIFICATION",
        "DELTA_CONCURRENT_DELETE_READ",
        "DELTA_CONCURRENT_DELETE_DELETE",
    )

    for retry in range(max_retries + 1):
        try:
            return acquire_attempt(
                spark=spark,
                batch_id=batch_id,
                lease_minutes=lease_minutes
            )

        except Exception as exc:
            is_concurrency_error = any(
                code in str(exc)
                for code in concurrency_errors
            )

            if not is_concurrency_error:
                raise

            if retry == max_retries:
                raise RuntimeError(
                    "Maximum concurrency retries exceeded"
                ) from exc

            delay = min(0.5 * (2 ** retry), 5.0)
            delay += random.uniform(0, 0.2)

            print(
                f"Delta concurrency conflict. "
                f"Retry {retry + 1}/{max_retries} "
                f"in {delay:.2f}s"
            )

            time.sleep(delay)


def mark_stage_completed(spark, batch_id, attempt_id, stage):
    stage_columns = {
        "BRONZE": "bronze_completed_at",
        "SILVER": "silver_completed_at",
        "GOLD": "gold_completed_at",
    }

    if stage not in stage_columns:
        raise ValueError(f"Invalid stage: {stage}")

    column = stage_columns[stage]

    result = spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            {column} = COALESCE(
                {column},
                current_timestamp()
            ),
            updated_at = current_timestamp()
        WHERE batch_id = :batch_id
          AND active_attempt_id = :attempt_id
          AND status = 'PROCESSING'
          AND lease_expires_at > current_timestamp()
    """, args={
        "batch_id": batch_id,
        "attempt_id": attempt_id,
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            f"Could not mark stage {stage} "
            f"as completed for batch {batch_id}"
        )

    print(f"{stage} AUDIT: UPDATED")


def update_batch_metrics(
    spark,
    batch_id,
    attempt_id,
    source_rows,
    valid_rows,
    invalid_rows
):
    if source_rows != valid_rows + invalid_rows:
        raise ValueError("Inconsistent batch metrics")

    result = spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            source_rows = :source_rows,
            valid_rows = :valid_rows,
            invalid_rows = :invalid_rows,
            updated_at = current_timestamp()
        WHERE batch_id = :batch_id
          AND active_attempt_id = :attempt_id
          AND status = 'PROCESSING'
          AND lease_expires_at > current_timestamp()
    """, args={
        "source_rows": int(source_rows),
        "valid_rows": int(valid_rows),
        "invalid_rows": int(invalid_rows),
        "batch_id": batch_id,
        "attempt_id": attempt_id,
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            f"Batch metrics update rejected: {batch_id}"
        )

    print("BATCH METRICS: UPDATED")


def assert_batch_ownership(spark, batch_id, attempt_id):
    batch = (
        spark.table(BATCHES_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("active_attempt_id") == attempt_id)
            & (F.col("status") == "PROCESSING")
            & (F.col("lease_expires_at") > F.current_timestamp())
        )
        .select("batch_id")
        .first()
    )

    if batch is None:
        raise RuntimeError(
            f"Batch ownership lost or lease expired: "
            f"batch_id={batch_id}, attempt_id={attempt_id}"
        )

    return True


def get_attempt_status(spark, attempt_id):
    attempt = (
        spark.table(ATTEMPTS_TABLE)
        .filter(F.col("attempt_id") == attempt_id)
        .select("status")
        .first()
    )

    if attempt is None:
        raise RuntimeError(
            f"Attempt not found: {attempt_id}"
        )

    return attempt.status

def reconcile_finalized_attempt(spark, batch_id):
    batch = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if batch is None:
        raise ValueError("Batch not found")

    if batch.status not in ("COMPLETED", "FAILED"):
        return False

    if batch.active_attempt_id is not None:
        raise RuntimeError(
            "Finalized batch still has active ownership"
        )

    attempts = (
        spark.table(ATTEMPTS_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("attempt_number") == batch.attempt_count)
        )
        .limit(2)
        .collect()
    )

    if len(attempts) != 1:
        raise RuntimeError(
            "Cannot identify a unique final attempt"
        )

    attempt = attempts[0]

    if attempt.status == batch.status:
        return False

    if attempt.status != "PROCESSING":
        raise RuntimeError(
            "Attempt has an incompatible final status"
        )

    if batch.status == "COMPLETED":
        if any(
            value is None
            for value in (
                batch.bronze_completed_at,
                batch.silver_completed_at,
                batch.gold_completed_at
            )
        ):
            raise RuntimeError(
                "Completed batch has missing stage timestamps"
            )

    result = spark.sql(f"""
        UPDATE {ATTEMPTS_TABLE}
        SET
            status = :status,
            finished_at = current_timestamp(),
            duration_seconds = timestampdiff(
                SECOND,
                started_at,
                current_timestamp()
            ),
            failed_stage = CASE
                WHEN :status = 'FAILED'
                THEN 'RECONCILIATION'
                ELSE NULL
            END,
            error_message = :error_message
        WHERE attempt_id = :attempt_id
          AND batch_id = :batch_id
          AND attempt_number = :attempt_number
          AND status = 'PROCESSING'
    """, args={
        "status": batch.status,
        "error_message": (
            batch.error_message
            if batch.status == "FAILED"
            else None
        ),
        "attempt_id": attempt.attempt_id,
        "batch_id": batch_id,
        "attempt_number": batch.attempt_count
    })

    if result.first().num_affected_rows != 1:
        raise RuntimeError(
            "Attempt reconciliation rejected"
        )

    print(
        f"Attempt reconciled: {attempt.attempt_id}, "
        f"status={batch.status}"
    )

    return True

