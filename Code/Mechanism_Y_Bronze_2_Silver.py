# Databricks notebook source
import logging
from pyspark.sql.functions import col, regexp_replace, when, count

# COMMAND ----------

# Setup logger (optional but good practice)
logger = logging.getLogger("Bronze_2_Silver")
logger.setLevel(logging.INFO)


# COMMAND ----------

# MAGIC %run ./audit_logger.py

# COMMAND ----------


# -------------------------------------
# 3. Read Bronze Delta Stream
# -------------------------------------

def read_bronze_stream():
    job_name = "READ_BRONZE_STREAM"
    try:
        logger.info(f"[{job_name}] Starting Delta stream read from Bronze layer...")
        log_audit(job_name=job_name, status="STARTED")

        df = spark.readStream.format("delta").load("abfss://bronze@devdolphinssa.dfs.core.windows.net/transactions/")
        
        logger.info(f"[{job_name}] Successfully defined Delta stream from Bronze layer.")
        log_audit(job_name=job_name, status="SUCCESS")
        return df

    except Exception as e:
        logger.error(f"[{job_name}] Failed to read Bronze Delta stream: {e}", exc_info=True)
        log_audit(job_name=job_name, status="FAILURE", error_message=str(e))
        None

# COMMAND ----------




# -------------------------------------
# 4. Clean and Transform Data
# -------------------------------------
def transform_data(df):
    job_name = "TRANSFORM_DATA"
    try:
        logger.info(f"[{job_name}] Starting data transformation...")
        log_audit(job_name=job_name, status="STARTED")

        transformed_df = df.withColumn("customer", regexp_replace(col("customer"), "'", "")) \
            .withColumn("gender", regexp_replace(col("gender"), "'", "")) \
            .withColumn("merchant", regexp_replace(col("merchant"), "'", "")) \
            .withColumn("amount", col("amount").cast("double")) \
            .drop("fraud", "step", "age", "zipcodeOri", "zipMerchant", "category")

        logger.info(f"[{job_name}] Data transformation completed successfully.")
        log_audit(job_name=job_name, status="SUCCESS")
        return transformed_df

    except Exception as e:
        logger.error(f"[{job_name}] Data transformation failed: {e}", exc_info=True)
        log_audit(job_name=job_name, status="FAILURE", error_message=str(e))
        raise


# COMMAND ----------


# # -------------------------------------
# # 5. Count Records in Stream
# # -------------------------------------
# def count_batch(batch_df, batch_id):
#     job_name = f"WRITE_BATCH_{batch_id}"
#     try:
#         count_value = batch_df.count()
#         logger.info(f"[{job_name}] Batch {batch_id} - Row count: {count_value}")
#         log_audit(job_name=job_name, status="STARTED", row_count=count_value)

#         batch_df.write.format("delta") \
#             .mode("append") \
#             .save("abfss://silver@devdolphinssa.dfs.core.windows.net/transactions/")

#         logger.info(f"[{job_name}] Batch {batch_id} written successfully to Silver layer.")
#         log_audit(job_name=job_name, status="SUCCESS")

#     except Exception as e:
#         logger.error(f"[{job_name}] Batch write failed: {e}", exc_info=True)
#         log_audit(job_name=job_name, status="FAILURE", error_message=str(e))
#         raise


# COMMAND ----------


# -------------------------------------
# 6. Write Stream to Silver
# -------------------------------------
def write_to_silver(df):
    job_name = "WRITE_TO_SILVER"
    try:
        logger.info(f"[{job_name}] Starting Silver stream write...")
        log_audit(job_name=job_name, status="STARTED")

        query = df.coalesce(1).writeStream \
            .option("checkpointLocation", "abfss://silver@devdolphinssa.dfs.core.windows.net/checkpoint") \
            .option("path", "abfss://silver@devdolphinssa.dfs.core.windows.net/transactions/") \
            .outputMode("append") \
            .start()
            # .foreachBatch(count_batch) \
        logger.info(f"[{job_name}] Silver stream write started successfully.")
        log_audit(job_name=job_name, status="SUCCESS")
        return query

    except Exception as e:
        logger.error(f"[{job_name}] Silver stream write failed: {e}", exc_info=True)
        log_audit(job_name=job_name, status="FAILURE", error_message=str(e))
        raise



# COMMAND ----------

try:
    df_raw = read_bronze_stream()
    logger.info("Successfully read stream from Bronze layer.")
except Exception as e:
    logger.error(f"Failed to read from Bronze stream: {e}")
    raise

try:
    df_clean = transform_data(df_raw)
except Exception as e:
    logger.error(f"Data transformation failed: {e}")
    raise

try:
    query = write_to_silver(df_clean)
    query.awaitTermination(30)
    logger.info("Writing to Silver layer completed.")
except Exception as e:
    logger.error(f"Failed to write to Silver layer: {e}")
    raise

logger.info("Bronze to Silver streaming pipeline completed successfully.")
