# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`abfss://raw@devdolphinssa.dfs.core.windows.net/transactions/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`abfss://bronze@devdolphinssa.dfs.core.windows.net/transactions/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`abfss://silver@devdolphinssa.dfs.core.windows.net/transactions/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.audit_log
# MAGIC

# COMMAND ----------

# MAGIC %sql select * from default.detection_results_table

# COMMAND ----------

df= spark.read.parquet('abfss://gold@devdolphinssa.dfs.core.windows.net/detections/detections_2025-07-18_00:16:06_0.parquet')
df.display()

# COMMAND ----------

