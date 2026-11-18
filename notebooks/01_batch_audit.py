# Databricks notebook source
CATALOG = "workspace"

BATCHES_TABLE = f"{CATALOG}.de_control.etl_batches"
ATTEMPTS_TABLE = f"{CATALOG}.de_control.etl_batch_attempts"

# COMMAND ----------

import uuid
import hashlib

from pyspark.sql import functions as F
from delta.tables import DeltaTable

BATCHES_TABLE = "workspace.de_control.etl_batches"
ATTEMPTS_TABLE = "workspace.de_control.etl_batch_attempts"

# COMMAND ----------

def start_batch(entity_name, source_file, input_sha256):

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

    spark.sql(f"""
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
            '{batch_id}',
            '{entity_name}',
            '{source_file}',
            '{input_sha256}',
            'PENDING',
            0,
            current_timestamp(),
            current_timestamp()
        )
    """)

    print(f"Batch created: {batch_id}")

    return batch_id

# COMMAND ----------

def start_attempt(batch_id):

    batch_rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .limit(1)
        .collect()
    )

    if not batch_rows:
        raise ValueError(f"Batch not found: {batch_id}")

    batch = batch_rows[0]

    if batch.status == "COMPLETED":
        print(f"Batch already completed: {batch_id}")
        return None

    if batch.status == "PROCESSING":
        raise ValueError(
            f"Batch already processing: {batch_id}"
        )

    attempt_number = batch.attempt_count + 1
    attempt_id = str(uuid.uuid4())

    spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'PROCESSING',
            attempt_count = {attempt_number},
            updated_at = current_timestamp(),
            error_message = NULL
        WHERE batch_id = '{batch_id}'
          AND status IN ('PENDING', 'FAILED')
          AND attempt_count = {batch.attempt_count}
    """)

    # Revalidar que el lote quedó reservado para este intento.
    updated = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if (
        updated.status != "PROCESSING"
        or updated.attempt_count != attempt_number
    ):
        raise RuntimeError("Could not start batch attempt")

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
            '{attempt_id}',
            '{batch_id}',
            {attempt_number},
            'PROCESSING',
            current_timestamp(),
            current_timestamp()
        )
    """)

    print(
        f"Attempt started: {attempt_id} "
        f"(number={attempt_number})"
    )

    return attempt_id

# COMMAND ----------

def fail_attempt(attempt_id, failed_stage, error_message):

    attempts = (
        spark.table(ATTEMPTS_TABLE)
        .filter(F.col("attempt_id") == attempt_id)
        .collect()
    )

    if len(attempts) != 1:
        raise ValueError("Attempt not found or duplicated")

    attempt = attempts[0]

    if attempt.status != "PROCESSING":
        raise ValueError(
            f"Attempt is not PROCESSING: {attempt.status}"
        )

    batch_id = attempt.batch_id

    # Utilizamos parámetros SQL para los valores dinámicos.
    spark.sql(f"""
        UPDATE {ATTEMPTS_TABLE}
        SET
            status = 'FAILED',
            finished_at = current_timestamp(),
            duration_seconds =
                unix_timestamp(current_timestamp()) -
                unix_timestamp(started_at),
            failed_stage = :failed_stage,
            error_message = :error_message
        WHERE attempt_id = :attempt_id
          AND status = 'PROCESSING'
    """, args={
        "failed_stage": failed_stage,
        "error_message": error_message,
        "attempt_id": attempt_id
    })

    spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'FAILED',
            error_message = :error_message,
            updated_at = current_timestamp()
        WHERE batch_id = :batch_id
          AND status = 'PROCESSING'
    """, args={
        "batch_id": batch_id,
        "error_message": error_message
    })

    print(f"Attempt failed: {attempt_id}")

# COMMAND ----------

def complete_attempt(attempt_id):

    rows = (
        spark.table(ATTEMPTS_TABLE)
        .filter(F.col("attempt_id") == attempt_id)
        .collect()
    )

    if len(rows) != 1:
        raise ValueError("Attempt not found or duplicated")

    attempt = rows[0]

    if attempt.status != "PROCESSING":
        raise ValueError(
            f"Attempt is not PROCESSING: {attempt.status}"
        )

    batch_rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == attempt.batch_id)
        .collect()
    )

    if len(batch_rows) != 1:
        raise ValueError("Batch not found or duplicated")

    batch = batch_rows[0]

    if (
        batch.status != "PROCESSING"
        or batch.attempt_count != attempt.attempt_number
    ):
        raise RuntimeError(
            "Attempt does not match the active batch execution"
        )

    spark.sql(f"""
        UPDATE {ATTEMPTS_TABLE}
        SET
            status = 'COMPLETED',
            finished_at = current_timestamp(),
            duration_seconds =
                unix_timestamp(current_timestamp()) -
                unix_timestamp(started_at),
            failed_stage = NULL,
            error_message = NULL
        WHERE attempt_id = :attempt_id
          AND status = 'PROCESSING'
    """, args={
        "attempt_id": attempt_id
    })

    print(f"Attempt completed: {attempt_id}")

# COMMAND ----------

def complete_batch(batch_id):

    batch_rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(batch_rows) != 1:
        raise ValueError("Batch not found or duplicated")

    batch = batch_rows[0]

    completed_attempts = (
        spark.table(ATTEMPTS_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("attempt_number") == batch.attempt_count)
            & (F.col("status") == "COMPLETED")
        )
        .count()
    )

    if completed_attempts != 1:
        raise RuntimeError(
            "The latest attempt must be COMPLETED"
        )

    spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'COMPLETED',
            updated_at = current_timestamp(),
            error_message = NULL
        WHERE batch_id = :batch_id
          AND status = 'PROCESSING'
    """, args={
        "batch_id": batch_id
    })

    print(f"Batch completed: {batch_id}")

# COMMAND ----------

def recover_batch(batch_id, stale_minutes=30):

    if stale_minutes <= 0:
        raise ValueError("stale_minutes must be positive")

    batches = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(batches) != 1:
        raise ValueError("Batch not found or duplicated")

    batch = batches[0]

    if batch.status != "PROCESSING":
        print("Batch does not require recovery")
        return False

    attempt_number = batch.attempt_count

    existing_attempts = (
        spark.table(ATTEMPTS_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("attempt_number") == attempt_number)
        )
        .collect()
    )

    if existing_attempts:
        print("Attempt exists; automatic recovery skipped")
        return False

    # Comprobar que el lote lleva suficiente tiempo sin actualizarse.
    stale = (
        spark.table(BATCHES_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("status") == "PROCESSING")
            & (F.col("attempt_count") == attempt_number)
            & (
                F.col("updated_at")
                < F.expr(
                    f"current_timestamp() - INTERVAL {int(stale_minutes)} MINUTES"
                )
            )
        )
        .count()
    )

    if stale != 1:
        print("Batch is not stale; recovery skipped")
        return False

    recovery_attempt_id = str(uuid.uuid4())

    # Registrar el intento que quedó sin historial.
    spark.sql(f"""
        INSERT INTO {ATTEMPTS_TABLE} (
            attempt_id,
            batch_id,
            attempt_number,
            status,
            started_at,
            finished_at,
            failed_stage,
            error_message,
            created_at
        )
        VALUES (
            :attempt_id,
            :batch_id,
            :attempt_number,
            'FAILED',
            current_timestamp(),
            current_timestamp(),
            'AUDIT',
            'Interrupted before attempt registration',
            current_timestamp()
        )
    """, args={
        "attempt_id": recovery_attempt_id,
        "batch_id": batch_id,
        "attempt_number": attempt_number
    })

    # Actualizar el lote a FAILED.
    spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'FAILED',
            error_message = 'Recovered interrupted attempt',
            updated_at = current_timestamp()
        WHERE batch_id = :batch_id
          AND status = 'PROCESSING'
          AND attempt_count = :attempt_number
    """, args={
        "batch_id": batch_id,
        "attempt_number": attempt_number
    })

    print(f"Recovered batch: {batch_id}")
    return True

# COMMAND ----------

def reconcile_batch(batch_id):

    batch_rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(batch_rows) != 1:
        raise ValueError("Batch not found or duplicated")

    batch = batch_rows[0]

    if batch.status != "PROCESSING":
        print("Batch is not PROCESSING; reconciliation skipped")
        return False

    attempt_rows = (
        spark.table(ATTEMPTS_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("attempt_number") == batch.attempt_count)
        )
        .collect()
    )

    if len(attempt_rows) != 1:
        print("Missing or duplicated attempt; reconciliation skipped")
        return False

    attempt = attempt_rows[0]

    if attempt.status not in ("FAILED", "COMPLETED"):
        print("Attempt is not terminal; reconciliation skipped")
        return False

    # Solo permitir la transición si el intento sigue siendo
    # el último intento registrado.
    spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = :new_status,
            error_message = :error_message,
            updated_at = current_timestamp()
        WHERE batch_id = :batch_id
          AND status = 'PROCESSING'
          AND attempt_count = :attempt_number
    """, args={
        "new_status": attempt.status,
        "error_message": (
            attempt.error_message
            if attempt.status == "FAILED"
            else None
        ),
        "batch_id": batch_id,
        "attempt_number": attempt.attempt_number
    })

    # Verificar el estado final.
    result = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if result.status != attempt.status:
        raise RuntimeError("Batch reconciliation was not confirmed")

    print(
        f"Batch reconciled: {batch_id}, "
        f"status={result.status}"
    )

    return True

# COMMAND ----------

import uuid

def acquire_batch_lease(batch_id, lease_minutes=30):

    if lease_minutes <= 0:
        raise ValueError("lease_minutes must be positive")

    attempt_id = str(uuid.uuid4())

    # Solo permitimos reservar lotes pendientes o fallidos.
    spark.sql(f"""
        UPDATE {BATCHES_TABLE}
        SET
            status = 'PROCESSING',
            active_attempt_id = :attempt_id,
            attempt_count = attempt_count + 1,
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
        "attempt_id": attempt_id,
        "batch_id": batch_id
    })

    # Comprobar si nuestra ejecución obtuvo la reserva.
    rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(rows) != 1:
        raise RuntimeError("Batch not found or duplicated")

    batch = rows[0]

    if (
        batch.active_attempt_id != attempt_id
        or batch.status != "PROCESSING"
    ):
        print("Lease not acquired")
        return None

    print(
        f"Lease acquired: {attempt_id}, "
        f"attempt_number={batch.attempt_count}"
    )

    return {
        "attempt_id": attempt_id,
        "attempt_number": batch.attempt_count
    }

# COMMAND ----------

def renew_batch_lease(
    batch_id,
    attempt_id,
    lease_minutes=30
):
    if lease_minutes <= 0:
        raise ValueError("lease_minutes must be positive")

    spark.sql(f"""
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

    # Verificar que la reserva siga perteneciendo al intento.
    rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(rows) != 1:
        raise RuntimeError("Batch not found or duplicated")

    batch = rows[0]

    if (
        batch.active_attempt_id != attempt_id
        or batch.status != "PROCESSING"
        or batch.lease_expires_at is None
        or batch.lease_expires_at <= batch.heartbeat_at
    ):
        raise RuntimeError(
            "Lease renewal could not be confirmed"
        )

    print(f"Lease renewal observed: {attempt_id}")

    return batch.lease_expires_at

# COMMAND ----------

def register_leased_attempt(batch_id, attempt_id, attempt_number):

    batches = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(batches) != 1:
        raise ValueError("Batch not found or duplicated")

    batch = batches[0]

    if (
        batch.status != "PROCESSING"
        or batch.active_attempt_id != attempt_id
        or batch.attempt_count != attempt_number
        or batch.lease_expires_at is None
    ):
        raise RuntimeError("Attempt does not own the batch")

    # Evitar registrar un intento ya existente.
    existing = (
        spark.table(ATTEMPTS_TABLE)
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("attempt_number") == attempt_number)
        )
        .collect()
    )

    if existing:
        if (
            len(existing) == 1
            and existing[0].attempt_id == attempt_id
        ):
            print("Attempt already registered")
            return False

        raise RuntimeError("Conflicting attempt registration")

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

    print(f"Attempt registered: {attempt_id}")
    return True

# COMMAND ----------

def recover_expired_lease(batch_id):

    rows = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if len(rows) != 1:
        raise RuntimeError("Batch not found or duplicated")

    batch = rows[0]

    # Validar que la reserva esté vencida usando el reloj de Spark.
    expired = spark.sql("""
        SELECT current_timestamp() AS now
    """).first().now

    if (
        batch.status != "PROCESSING"
        or batch.active_attempt_id is None
        or batch.lease_expires_at is None
        or batch.lease_expires_at >= expired
    ):
        print("No expired lease to recover")
        return False

    attempt_id = batch.active_attempt_id
    attempt_number = batch.attempt_count

    # Por ahora recuperamos únicamente el caso sin intento registrado.
    existing = (
        spark.table(ATTEMPTS_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .collect()
    )

    if existing:
        raise RuntimeError(
            "Existing attempts require a different recovery path"
        )

    # Invalidar la reserva solamente si sigue siendo la misma.
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

    # Comprobar que el UPDATE afectó exactamente una fila.
    affected = result.first().num_affected_rows

    if affected != 1:
        print("Lease changed before recovery")
        return False

    # Registrar el intento interrumpido.
    spark.sql(f"""
        INSERT INTO {ATTEMPTS_TABLE} (
            attempt_id,
            batch_id,
            attempt_number,
            status,
            started_at,
            finished_at,
            failed_stage,
            error_message,
            created_at
        )
        VALUES (
            :attempt_id,
            :batch_id,
            :attempt_number,
            'FAILED',
            current_timestamp(),
            current_timestamp(),
            'LEASE',
            'Lease expired before attempt registration',
            current_timestamp()
        )
    """, args={
        "attempt_id": attempt_id,
        "batch_id": batch_id,
        "attempt_number": attempt_number
    })

    print(f"Expired lease recovered: {attempt_id}")
    return True

# COMMAND ----------

def acquire_attempt(batch_id, lease_minutes=30):

    if lease_minutes <= 0:
        raise ValueError("lease_minutes must be positive")

    attempt_id = str(uuid.uuid4())

    # 1. Intentar adquirir la reserva.
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

    affected = result.first().num_affected_rows

    if affected != 1:
        print("Attempt acquisition rejected")
        return None

    # 2. Recuperar el número asignado.
    batch = (
        spark.table(BATCHES_TABLE)
        .filter(F.col("batch_id") == batch_id)
        .first()
    )

    if (
        batch is None
        or batch.active_attempt_id != attempt_id
    ):
        raise RuntimeError(
            "Lost ownership after acquisition"
        )

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

# COMMAND ----------

def finish_attempt(
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

    # 1. Liberar la reserva únicamente si pertenece
    #    al intento que está finalizando.
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

    affected = result.first().num_affected_rows

    if affected != 1:
        raise RuntimeError(
            "Attempt cannot finalize: ownership lost "
            "or lease expired"
        )

    # 2. Actualizar el historial del intento.
    result = spark.sql(f"""
        UPDATE {ATTEMPTS_TABLE}
        SET
            status = :status,
            finished_at = current_timestamp(),
            duration_seconds =
                timestampdiff(
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

    print(
        f"Attempt finalized: {attempt_id}, status={status}"
    )

    return True

# COMMAND ----------

import time
import random

def acquire_attempt_with_retry(
    batch_id,
    lease_minutes=30,
    max_retries=3
):
    if max_retries < 0:
        raise ValueError("max_retries cannot be negative")

    for retry in range(max_retries + 1):
        try:
            return acquire_attempt(
                batch_id=batch_id,
                lease_minutes=lease_minutes
            )

        except Exception as e:
            error_text = str(e)

            is_concurrency_error = any(
                code in error_text
                for code in (
                    "DELTA_CONCURRENT_APPEND",
                    "DELTA_CONCURRENT_MODIFICATION",
                    "DELTA_CONCURRENT_DELETE_READ",
                    "DELTA_CONCURRENT_DELETE_DELETE",
                )
            )

            if not is_concurrency_error:
                raise

            if retry == max_retries:
                raise RuntimeError(
                    "Maximum concurrency retries exceeded"
                ) from e

            delay = min(
                0.5 * (2 ** retry),
                5.0
            ) + random.uniform(0, 0.2)

            print(
                f"Delta concurrency conflict. "
                f"Retry {retry + 1}/{max_retries} "
                f"in {delay:.2f}s"
            )

            time.sleep(delay)

# COMMAND ----------

def recover_expired_registered_attempt(batch_id):
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
            (F.col("attempt_id") == attempt_id) &
            (F.col("batch_id") == batch_id) &
            (F.col("attempt_number") == attempt_number)
        )
        .first()
    )

    if attempt is None or attempt.status != "PROCESSING":
        raise RuntimeError(
            "Attempt audit inconsistent; reconciliation required"
        )

    # Recuperar únicamente si el lease sigue vencido
    # y el intento sigue siendo el propietario.
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

    # Registrar el fallo del intento anterior.
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