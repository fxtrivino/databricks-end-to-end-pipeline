# Databricks notebook source
dbutils.widgets.text("source_file", "")
dbutils.widgets.text("lease_minutes", "60")

source_file = dbutils.widgets.get("source_file")
lease_minutes = int(dbutils.widgets.get("lease_minutes"))

if not source_file:
    raise ValueError("source_file is required")

if lease_minutes <= 0:
    raise ValueError("lease_minutes must be positive")

print("Source file:", source_file)
print("Lease minutes:", lease_minutes)

# COMMAND ----------

# MAGIC %run ./14_customers_job

# COMMAND ----------

import json

result = run_customers_job(
    source_file=source_file,
    lease_minutes=lease_minutes
)

print("JOB RESULT:", json.dumps(result))