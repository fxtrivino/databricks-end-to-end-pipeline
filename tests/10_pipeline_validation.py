# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.country,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(o.total_amount) AS total_amount
# MAGIC FROM workspace.de_gold.fact_orders o
# MAGIC JOIN workspace.de_gold.dim_customers c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC WHERE o.status = 'CONFIRMED'
# MAGIC GROUP BY c.country
# MAGIC ORDER BY c.country;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH payments_summary AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN status = 'COMPLETED' THEN amount
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS paid_amount,
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN status = 'PENDING' THEN amount
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS pending_amount,
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN status = 'FAILED' THEN amount
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS failed_amount
# MAGIC     FROM workspace.de_gold.fact_payments
# MAGIC     GROUP BY order_id
# MAGIC )
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     o.status AS order_status,
# MAGIC     o.total_amount,
# MAGIC     COALESCE(p.paid_amount, 0) AS paid_amount,
# MAGIC     COALESCE(p.pending_amount, 0) AS pending_amount,
# MAGIC     COALESCE(p.failed_amount, 0) AS failed_amount,
# MAGIC     o.total_amount - COALESCE(p.paid_amount, 0) AS outstanding_amount
# MAGIC FROM workspace.de_gold.fact_orders o
# MAGIC LEFT JOIN payments_summary p
# MAGIC     ON o.order_id = p.order_id
# MAGIC ORDER BY o.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_payments,
# MAGIC     SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END)
# MAGIC         AS collected_amount,
# MAGIC     SUM(CASE WHEN status = 'PENDING' THEN amount ELSE 0 END)
# MAGIC         AS pending_amount,
# MAGIC     SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END)
# MAGIC         AS failed_amount
# MAGIC FROM workspace.de_gold.fact_payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(o.total_amount) AS total_sales
# MAGIC FROM workspace.de_gold.fact_orders o
# MAGIC JOIN workspace.de_gold.fact_payments p
# MAGIC     ON o.order_id = p.order_id
# MAGIC WHERE o.status = 'CONFIRMED';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *--SUM(o.total_amount) AS total_sales
# MAGIC FROM workspace.de_gold.fact_orders o
# MAGIC JOIN workspace.de_gold.fact_payments p
# MAGIC     ON o.order_id = p.order_id
# MAGIC WHERE o.status = 'CONFIRMED';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(o.total_amount) AS total_sales
# MAGIC FROM workspace.de_gold.fact_orders o
# MAGIC WHERE o.status = 'CONFIRMED'
# MAGIC   AND EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM workspace.de_gold.fact_payments p
# MAGIC       WHERE p.order_id = o.order_id
# MAGIC   );