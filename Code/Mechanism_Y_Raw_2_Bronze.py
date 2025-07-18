# Databricks notebook source
import logging

# COMMAND ----------

# MAGIC %run ./audit_logger.py

# COMMAND ----------

# Setup logger (optional but good practice)
logger = logging.getLogger("Raw_2_Bronze")
logger.setLevel(logging.INFO)

# COMMAND ----------

# def count_batch(batch_df, batch_id):
#     job_name = f"WRITE_BATCH_{batch_id}"
#     try:
#         count_value = batch_df.count()
#         logger.info(f"[{job_name}] Batch {batch_id} - Row count: {count_value}")
#         log_audit(job_name=job_name, status="STARTED", row_count=count_value)

#         batch_df.write.format("delta") \
#             .mode("append") \
#             .save("abfss://bronze@devdolphinssa.dfs.core.windows.net/transactions/")

#         logger.info(f"[{job_name}] Batch {batch_id} written successfully to Silver layer.")
#         log_audit(job_name=job_name, status="SUCCESS")

#     except Exception as e:
#         logger.error(f"[{job_name}] Batch write failed: {e}", exc_info=True)
#         log_audit(job_name=job_name, status="FAILURE", error_message=str(e))
#         raise

# COMMAND ----------


try:
    job_name = "RAW_2_BRONZE_STREAM"
    logger.info("Starting streaming read from raw layer...")
    log_audit(job_name, status="STARTED")



    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", True) \
        .option("cloudFiles.schemaLocation", "abfss://bronze@devdolphinssa.dfs.core.windows.net/schema/checkpoint") \
        .load("abfss://raw@devdolphinssa.dfs.core.windows.net/transactions/")

    logger.info("Successfully defined read stream from raw layer to bronze layer")

    query = df.coalesce(1).writeStream.format("delta") \
        .option("checkpointLocation", "abfss://bronze@devdolphinssa.dfs.core.windows.net/checkpoint") \
        .option("path", "abfss://bronze@devdolphinssa.dfs.core.windows.net/transactions/") \
        .outputMode("append") \
        .start()

# .foreachBatch(count_batch) \

    logger.info("Streaming write started. Waiting for completion...")
    query.awaitTermination(30)

    logger.info("Streaming write completed successfully.")
    log_audit(job_name, status="SUCCESS")

except Exception as e:
    logger.error(f"Streaming pipeline failed: {e}", exc_info=True)
    log_audit(job_name, status="FAILURE", error_message=str(e))
    raise



# COMMAND ----------

