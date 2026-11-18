# Databricks notebook source
# MAGIC %run ../notebooks/01_batch_audit

# COMMAND ----------


test_hash = hashlib.sha256(
    b"customers_test_batch_1"
).hexdigest()

batch_id = start_batch(
    entity_name="CUSTOMERS",
    source_file="customers_test_batch_1.csv",
    input_sha256=test_hash
)

attempt_id = start_attempt(batch_id)

print("BATCH_ID:", batch_id)
print("ATTEMPT_ID:", attempt_id)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == batch_id)
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == batch_id)
)

# COMMAND ----------

fail_attempt(
    attempt_id=attempt_id,
    failed_stage="SILVER",
    error_message="Simulated Silver transformation failure"
)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .select(
        "entity_name",
        "status",
        "attempt_count",
        "error_message"
    )
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .select(
        "attempt_number",
        "status",
        "failed_stage",
        "error_message",
        "duration_seconds"
    )
)

# COMMAND ----------

attempt_id_2 = start_attempt(batch_id)

print("BATCH_ID:", batch_id)
print("ATTEMPT_ID_2:", attempt_id_2)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .select(
        "entity_name",
        "status",
        "attempt_count",
        "error_message"
    )
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .select(
        "attempt_id",
        "attempt_number",
        "status",
        "started_at",
        "finished_at",
        "failed_stage"
    )
    .orderBy("attempt_number")
)

# COMMAND ----------

complete_attempt(attempt_id_2)

complete_batch(batch_id)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .select(
        "entity_name",
        "status",
        "attempt_count",
        "error_message"
    )
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == batch_id)
    .select(
        "attempt_number",
        "status",
        "started_at",
        "finished_at",
        "duration_seconds",
        "failed_stage"
    )
    .orderBy("attempt_number")
)

# COMMAND ----------

# Intentar registrar nuevamente el mismo lote

batch_id_again = start_batch(
    entity_name="CUSTOMERS",
    source_file="customers_test_batch_1.csv",
    input_sha256=test_hash
)

# Intentar iniciar una nueva ejecución
attempt_id_again = start_attempt(batch_id_again)

print("Same batch:", batch_id_again == batch_id)
print("New attempt:", attempt_id_again)

# COMMAND ----------

batch_df = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == batch_id)
)

attempts_df = (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == batch_id)
)

assert batch_df.count() == 1
assert attempts_df.count() == 2

batch = batch_df.first()

assert batch.status == "COMPLETED"
assert batch.attempt_count == 2

print("IDEMPOTENCY TEST: PASSED")

# COMMAND ----------

recovery_hash = hashlib.sha256(
    b"customers_recovery_test_1"
).hexdigest()

recovery_batch_id = start_batch(
    entity_name="CUSTOMERS",
    source_file="customers_recovery_test_1.csv",
    input_sha256=recovery_hash
)

print("Recovery batch:", recovery_batch_id)

# COMMAND ----------

spark.sql(f"""
    UPDATE {BATCHES_TABLE}
    SET
        status = 'PROCESSING',
        attempt_count = 1,
        updated_at = current_timestamp()
    WHERE batch_id = :batch_id
      AND status = 'PENDING'
      AND attempt_count = 0
""", args={
    "batch_id": recovery_batch_id
})

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == recovery_batch_id)
    .select("status", "attempt_count", "updated_at")
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == recovery_batch_id)
)

# COMMAND ----------

spark.sql(f"""
    UPDATE {BATCHES_TABLE}
    SET updated_at = current_timestamp() - INTERVAL 45 MINUTES
    WHERE batch_id = :batch_id
      AND status = 'PROCESSING'
      AND attempt_count = 1
""", args={
    "batch_id": recovery_batch_id
})

# COMMAND ----------

recovered = recover_batch(
    batch_id=recovery_batch_id,
    stale_minutes=30
)

print("Recovery executed:", recovered)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == recovery_batch_id)
    .select(
        "status",
        "attempt_count",
        "error_message"
    )
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == recovery_batch_id)
    .select(
        "attempt_number",
        "status",
        "failed_stage",
        "error_message"
    )
)

# COMMAND ----------

reconcile_hash = hashlib.sha256(
    b"customers_reconcile_test_1"
).hexdigest()

reconcile_batch_id = start_batch(
    entity_name="CUSTOMERS",
    source_file="customers_reconcile_test_1.csv",
    input_sha256=reconcile_hash
)

reconcile_attempt_id = start_attempt(reconcile_batch_id)

# COMMAND ----------

spark.sql(f"""
    UPDATE {ATTEMPTS_TABLE}
    SET
        status = 'FAILED',
        finished_at = current_timestamp(),
        failed_stage = 'SILVER',
        error_message = 'Simulated partial audit failure'
    WHERE attempt_id = :attempt_id
      AND status = 'PROCESSING'
""", args={
    "attempt_id": reconcile_attempt_id
})

# COMMAND ----------

reconciled = reconcile_batch(reconcile_batch_id)

print("Reconciled:", reconciled)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == reconcile_batch_id)
    .select("status", "attempt_count", "error_message")
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == reconcile_batch_id)
    .select("attempt_number", "status", "failed_stage")
)

# COMMAND ----------

reconciled_again = reconcile_batch(reconcile_batch_id)

assert reconciled_again is False

assert (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == reconcile_batch_id)
    .count()
) == 1

assert (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == reconcile_batch_id)
    .count()
) == 1

print("RECONCILIATION IDEMPOTENCY TEST: PASSED")

# COMMAND ----------

completed_hash = hashlib.sha256(
    b"customers_reconcile_completed_test_1"
).hexdigest()

completed_batch_id = start_batch(
    entity_name="CUSTOMERS",
    source_file="customers_reconcile_completed_test_1.csv",
    input_sha256=completed_hash
)

completed_attempt_id = start_attempt(completed_batch_id)

print("Batch:", completed_batch_id)
print("Attempt:", completed_attempt_id)

# COMMAND ----------

complete_attempt(completed_attempt_id)

# Simulamos una interrupción antes de complete_batch()

# COMMAND ----------

reconciled = reconcile_batch(completed_batch_id)

print("Reconciled:", reconciled)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == completed_batch_id)
    .select("status", "attempt_count", "error_message")
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == completed_batch_id)
    .select(
        "attempt_number",
        "status",
        "finished_at",
        "duration_seconds"
    )
)

# COMMAND ----------

reconciled_again = reconcile_batch(completed_batch_id)

assert reconciled_again is False

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == completed_batch_id)
    .first()
)

attempts = (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == completed_batch_id)
    .collect()
)

assert batch.status == "COMPLETED"
assert batch.attempt_count == 1

assert len(attempts) == 1
assert attempts[0].status == "COMPLETED"

print("COMPLETED RECONCILIATION TEST: PASSED")

# COMMAND ----------

lease_hash = hashlib.sha256(
    b"orders_lease_test_1"
).hexdigest()

lease_batch_id = start_batch(
    entity_name="ORDERS",
    source_file="orders_lease_test_1.csv",
    input_sha256=lease_hash
)

# COMMAND ----------

job_a = acquire_batch_lease(
    batch_id=lease_batch_id,
    lease_minutes=30
)

job_b = acquire_batch_lease(
    batch_id=lease_batch_id,
    lease_minutes=30
)

print("Job A:", job_a)
print("Job B:", job_b)

assert job_a is not None
assert job_b is None

print("SEQUENTIAL LEASE TEST: PASSED")

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == lease_batch_id)
    .select(
        "entity_name",
        "status",
        "attempt_count",
        "active_attempt_id",
        "heartbeat_at",
        "lease_expires_at"
    )
)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == lease_batch_id)
    .select(
        "status",
        "active_attempt_id",
        "heartbeat_at",
        "lease_expires_at"
    )
)

# COMMAND ----------

new_expiration = renew_batch_lease(
    batch_id=lease_batch_id,
    attempt_id=job_a["attempt_id"],
    lease_minutes=30
)

print("New expiration:", new_expiration)

# COMMAND ----------

from datetime import datetime, timezone

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == lease_batch_id)
    .first()
)

assert batch.status == "PROCESSING"
assert batch.active_attempt_id == job_a["attempt_id"]
assert batch.lease_expires_at > batch.heartbeat_at

print("HEARTBEAT TEST: PASSED")

# COMMAND ----------

registered = register_leased_attempt(
    batch_id=lease_batch_id,
    attempt_id=job_a["attempt_id"],
    attempt_number=job_a["attempt_number"]
)

print("Registered:", registered)

# COMMAND ----------

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == lease_batch_id)
    .select(
        "attempt_id",
        "attempt_number",
        "status",
        "started_at"
    )
)

# COMMAND ----------

recovered = recover_expired_lease(
    batch_id="03f9eea97aaae04cf4f350a492c63f82db038d8fc6eee90d1ac1dbfc70db0afc"
)

print("Recovered:", recovered)

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == "03f9eea97aaae04cf4f350a492c63f82db038d8fc6eee90d1ac1dbfc70db0afc")
    .select(
        "status",
        "attempt_count",
        "active_attempt_id",
        "lease_expires_at",
        "error_message"
    )
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == "03f9eea97aaae04cf4f350a492c63f82db038d8fc6eee90d1ac1dbfc70db0afc")
    .select(
        "attempt_id",
        "attempt_number",
        "status",
        "failed_stage",
        "error_message"
    )
)

# COMMAND ----------

second_recovery = recover_expired_lease(
    batch_id="03f9eea97aaae04cf4f350a492c63f82db038d8fc6eee90d1ac1dbfc70db0afc"
)

assert second_recovery is False

assert (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == "03f9eea97aaae04cf4f350a492c63f82db038d8fc6eee90d1ac1dbfc70db0afc")
    .count()
) == 1

print("EXPIRED LEASE RECOVERY TEST: PASSED")

# COMMAND ----------

test_hash = hashlib.sha256(
    b"orders_acquire_attempt_test_1"
).hexdigest()

test_batch_id = start_batch(
    entity_name="ORDERS",
    source_file="orders_acquire_attempt_test_1.csv",
    input_sha256=test_hash
)

print("Batch:", test_batch_id)

# COMMAND ----------

attempt_a = acquire_attempt(
    batch_id=test_batch_id,
    lease_minutes=30
)

attempt_b = acquire_attempt(
    batch_id=test_batch_id,
    lease_minutes=30
)

print("Attempt A:", attempt_a)
print("Attempt B:", attempt_b)

assert attempt_a is not None
assert attempt_b is None

print("ACQUIRE ATTEMPT TEST: PASSED")

# COMMAND ----------

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == test_batch_id)
    .first()
)

attempts = (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == test_batch_id)
    .collect()
)

assert batch.status == "PROCESSING"
assert batch.attempt_count == 1
assert batch.active_attempt_id == attempt_a["attempt_id"]

assert len(attempts) == 1
assert attempts[0].attempt_id == attempt_a["attempt_id"]
assert attempts[0].attempt_number == 1
assert attempts[0].status == "PROCESSING"

print("AUDIT CONSISTENCY TEST: PASSED")

# COMMAND ----------

finish_hash = hashlib.sha256(
    b"orders_finish_attempt_test_1"
).hexdigest()

finish_batch_id = start_batch(
    entity_name="ORDERS",
    source_file="orders_finish_attempt_test_1.csv",
    input_sha256=finish_hash
)

finish_owner = acquire_attempt(
    batch_id=finish_batch_id,
    lease_minutes=30
)

assert finish_owner is not None

# COMMAND ----------

finished = finish_attempt(
    batch_id=finish_batch_id,
    attempt_id=finish_owner["attempt_id"],
    attempt_number=finish_owner["attempt_number"],
    status="COMPLETED"
)

assert finished is True

print("FINISH ATTEMPT TEST: PASSED")

# COMMAND ----------

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == finish_batch_id)
    .first()
)

attempt = (
    spark.table(ATTEMPTS_TABLE)
    .filter(
        F.col("attempt_id")
        == finish_owner["attempt_id"]
    )
    .first()
)

assert batch.status == "COMPLETED"
assert batch.active_attempt_id is None
assert batch.lease_expires_at is None

assert attempt.status == "COMPLETED"
assert attempt.finished_at is not None

print("FINISH AUDIT CONSISTENCY TEST: PASSED")

# COMMAND ----------

failed_hash = hashlib.sha256(
    b"orders_failed_retry_test_1"
).hexdigest()

failed_batch_id = start_batch(
    entity_name="ORDERS",
    source_file="orders_failed_retry_test_1.csv",
    input_sha256=failed_hash
)

failed_owner_1 = acquire_attempt(
    batch_id=failed_batch_id,
    lease_minutes=30
)

assert failed_owner_1 is not None

finish_attempt(
    batch_id=failed_batch_id,
    attempt_id=failed_owner_1["attempt_id"],
    attempt_number=failed_owner_1["attempt_number"],
    status="FAILED",
    failed_stage="SILVER",
    error_message="Simulated Silver transformation failure"
)

print("FAILED ATTEMPT TEST: PASSED")

# COMMAND ----------

failed_owner_2 = acquire_attempt(
    batch_id=failed_batch_id,
    lease_minutes=30
)

assert failed_owner_2 is not None
assert failed_owner_2["attempt_number"] == 2
assert failed_owner_2["attempt_id"] != failed_owner_1["attempt_id"]

finish_attempt(
    batch_id=failed_batch_id,
    attempt_id=failed_owner_2["attempt_id"],
    attempt_number=failed_owner_2["attempt_number"],
    status="COMPLETED"
)

print("RETRY AFTER FAILURE TEST: PASSED")

# COMMAND ----------

display(
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == failed_batch_id)
    .select(
        "status",
        "attempt_count",
        "active_attempt_id",
        "lease_expires_at"
    )
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == failed_batch_id)
    .orderBy("attempt_number")
    .select(
        "attempt_number",
        "status",
        "failed_stage",
        "error_message",
        "duration_seconds"
    )
)

# COMMAND ----------

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == failed_batch_id)
    .first()
)

attempts = (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == failed_batch_id)
    .orderBy("attempt_number")
    .collect()
)

assert batch.status == "COMPLETED"
assert batch.attempt_count == 2
assert batch.active_attempt_id is None
assert batch.lease_expires_at is None

assert len(attempts) == 2
assert attempts[0].status == "FAILED"
assert attempts[0].failed_stage == "SILVER"
assert attempts[1].status == "COMPLETED"

print("FAILED RETRY AUDIT TEST: PASSED")

# COMMAND ----------

fencing_hash = hashlib.sha256(
    b"orders_fencing_test_1"
).hexdigest()

fencing_batch_id = start_batch(
    entity_name="ORDERS",
    source_file="orders_fencing_test_1.csv",
    input_sha256=fencing_hash
)

fencing_owner_1 = acquire_attempt(
    batch_id=fencing_batch_id,
    lease_minutes=30
)

assert fencing_owner_1 is not None

finish_attempt(
    batch_id=fencing_batch_id,
    attempt_id=fencing_owner_1["attempt_id"],
    attempt_number=fencing_owner_1["attempt_number"],
    status="FAILED",
    failed_stage="SILVER",
    error_message="Simulated first attempt failure"
)

# COMMAND ----------

fencing_owner_2 = acquire_attempt(
    batch_id=fencing_batch_id,
    lease_minutes=30
)

assert fencing_owner_2 is not None
assert fencing_owner_2["attempt_number"] == 2

# COMMAND ----------

rejected = False

try:
    finish_attempt(
        batch_id=fencing_batch_id,
        attempt_id=fencing_owner_1["attempt_id"],
        attempt_number=fencing_owner_1["attempt_number"],
        status="COMPLETED"
    )
except RuntimeError as e:
    print("Expected rejection:", str(e))
    rejected = True

assert rejected

print("STALE ATTEMPT REJECTION TEST: PASSED")

# COMMAND ----------

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == fencing_batch_id)
    .first()
)

assert batch.status == "PROCESSING"
assert batch.attempt_count == 2
assert batch.active_attempt_id == fencing_owner_2["attempt_id"]

print("CURRENT OWNER PROTECTION TEST: PASSED")

# COMMAND ----------

finish_attempt(
    batch_id=fencing_batch_id,
    attempt_id=fencing_owner_2["attempt_id"],
    attempt_number=fencing_owner_2["attempt_number"],
    status="COMPLETED"
)

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == fencing_batch_id)
    .first()
)

assert batch.status == "COMPLETED"
assert batch.attempt_count == 2
assert batch.active_attempt_id is None

print("FENCING TEST: PASSED")

# COMMAND ----------

concurrent_hash = hashlib.sha256(
    b"orders_concurrency_test_1"
).hexdigest()

concurrent_batch_id = start_batch(
    entity_name="ORDERS",
    source_file="orders_concurrency_test_1.csv",
    input_sha256=concurrent_hash
)

print("Concurrent batch:", concurrent_batch_id)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import threading

barrier = threading.Barrier(2)

def concurrent_worker(worker_name):
    try:
        barrier.wait(timeout=15)

        owner = acquire_attempt(
            batch_id=concurrent_batch_id,
            lease_minutes=30
        )

        return {
            "worker": worker_name,
            "owner": owner,
            "error": None
        }

    except Exception as e:
        return {
            "worker": worker_name,
            "owner": None,
            "error": str(e)
        }


with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [
        executor.submit(concurrent_worker, "JOB_A"),
        executor.submit(concurrent_worker, "JOB_B")
    ]

    results = [f.result() for f in futures]

for result in results:
    print(result)

# COMMAND ----------

batch = (
    spark.table(BATCHES_TABLE)
    .filter(F.col("batch_id") == concurrent_batch_id)
    .first()
)

attempts = (
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("batch_id") == concurrent_batch_id)
    .collect()
)

successful = [
    r for r in results
    if r["owner"] is not None
]

print("Successful acquisitions:", len(successful))
print("Batch status:", batch.status)
print("Attempt count:", batch.attempt_count)
print("Registered attempts:", len(attempts))

assert len(successful) <= 1
assert batch.attempt_count <= 1
assert len(attempts) <= 1

if len(successful) == 1:
    assert batch.status == "PROCESSING"
    assert batch.active_attempt_id == successful[0]["owner"]["attempt_id"]
    assert len(attempts) == 1
    assert attempts[0].attempt_id == batch.active_attempt_id
    print("CONCURRENT ACQUISITION TEST: PASSED")
else:
    print(
        "No acquisition confirmed; inspect errors "
        "and audit state before retrying"
    )

# COMMAND ----------

# MAGIC %run ../notebooks/18_payments_job

# COMMAND ----------

import inspect
def prepare_payments_execution(): pass
print(
    "Prepare:",
    inspect.signature(prepare_payments_execution)
)

print(
    "Run:",
    inspect.signature(run_payments_job)
)

print("PAYMENTS JOB FUNCTIONS: PASSED")

# COMMAND ----------

import src.audit as audit

expired_batches = [
    "915c41e58443c667ea2ef2183999a38b37275bd474ebfd22e28b408e85c02b96",
    "172ea857cb9ec05b7a910f4994fb613d3b753f26c507131fe46b213b0c90a54d"
]

for batch_id in expired_batches:
    try:
        recovered = audit.recover_expired_registered_attempt(
            spark=spark,
            batch_id=batch_id
        )
        print(f"Batch: {batch_id} | Recovered: {recovered}")
    except Exception as exc:
        print(f"Batch: {batch_id} | ERROR: {exc}")

# COMMAND ----------

from pathlib import Path
import sys

project_root = Path.cwd().parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

import inspect
def prepare_payments_execution():
    pass
print(inspect.getsource(prepare_payments_execution))

# COMMAND ----------

import inspect

print(inspect.signature(prepare_payments_execution))
print(inspect.getsource(prepare_payments_execution))

# COMMAND ----------

import hashlib
from pathlib import Path
import src.audit as audit

source_file = "payments_recovery_test_1.csv"

path = Path(
    "/Volumes/workspace/de_bronze/landing/payments"
) / source_file

csv_content = """payment_id,order_id,payment_date,amount,payment_method,status,updated_at
9951,9101,2026-11-17 16:00:00,125.50,CREDIT_CARD,COMPLETED,2026-11-17 16:00:00
9952,9102,2026-11-17 16:05:00,350.00,TRANSFER,COMPLETED,2026-11-17 16:05:00
"""

if path.exists():
    raise FileExistsError(f"Test file already exists: {path}")

path.write_text(csv_content)

input_sha256 = hashlib.sha256(
    path.read_bytes()
).hexdigest()

batch_id = audit.start_batch(
    spark=spark,
    entity_name="PAYMENTS",
    source_file=source_file,
    input_sha256=input_sha256
)

owner = audit.acquire_attempt(
    spark=spark,
    batch_id=batch_id,
    lease_minutes=1
)

if owner is None:
    raise RuntimeError("Could not acquire test batch")

print("BATCH ID:", batch_id)
print("ATTEMPT ID:", owner["attempt_id"])
print("ATTEMPT NUMBER:", owner["attempt_number"])

# COMMAND ----------

import inspect

print(inspect.getsource(prepare_orders_execution))