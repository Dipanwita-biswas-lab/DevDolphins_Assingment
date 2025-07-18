# Databricks notebook source

from pyspark.sql.functions import col, count, avg, lit, percent_rank, current_timestamp, countDistinct, monotonically_increasing_id, floor, row_number
from pyspark.sql.window import Window
import uuid
import time
import logging
from datetime import datetime, timedelta, timezone
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable



# COMMAND ----------

# MAGIC %md
# MAGIC Mechanism Y is a coordinated streaming pipeline that watches for new chunk files on ADLS, runs real-time pattern detection logic on incoming data, and exports detections to Gold storage in 50-record chunks per unique file. It uses a Delta-based staging table to manage export thresholds and avoid duplication or loss.
# MAGIC
# MAGIC

# COMMAND ----------

# --------------------------------------
# Logger Configuration
# --------------------------------------
logger = logging.getLogger("DetectionJob")
logger.setLevel(logging.INFO)


# COMMAND ----------

# MAGIC %run ./audit_logger.py

# COMMAND ----------

# Patterns PatId1 - 
# A customer in the top 10 percentile for a given merchant for the total number of transactions
# and in bottom 10% percentile weight averaged over all transaction types, merchant wants to UPGRADE(actionType) them. 
# Upgradation only begins once total transactions for the merchant exceed 50K. 

def detect_patid1(df,  min_txns: int = 50000,
                  top_txn_pct: float = 0.9,
                  low_value_pct: float = 0.1,
                  pattern_id: str = "PatId1",
                  action_type: str = "UPGRADE"):
    
    try:
        logger.info("Starting PatId1 detection")

        # Step 1: Filter merchants with > 50,000 transactions
        logger.info(f"Filtering merchants with more than {min_txns} transactions")
        merchant_txn_counts = df.groupBy("merchant").agg(count("*").alias("total_txns"))
        eligible_merchants = merchant_txn_counts.filter(col("total_txns") > min_txns)

        df_filtered = df.join(eligible_merchants, "merchant", how="inner")

        # Step 2: Compute transaction count and average value per customer per merchant
        customer_stats = df_filtered.groupBy("merchant", "customer").agg(count("*").alias("txn_count"), avg("amount").alias("avg_value"))

        # Step 3: Compute percentiles using window functions
        txn_window = Window.partitionBy("merchant").orderBy(col("txn_count").desc())
        value_window = Window.partitionBy("merchant").orderBy(col("avg_value").asc())

        customer_stats = customer_stats.withColumn("txn_percentile", percent_rank().over(txn_window)) \
        .withColumn("value_percentile", percent_rank().over(value_window))

        # Get current IST time
        ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        ist_str = ist_now.strftime("%Y-%m-%d %H:%M:%S")

        # Step 4: Filter for top 10% in txn count and bottom 10% in avg value
        patid1_detections = customer_stats.filter((col("txn_percentile") >= top_txn_pct) & (col("value_percentile") <= low_value_pct)) \
            .select( lit(pattern_id).alias("patternId"),
                    lit(action_type).alias("ActionType"),
                col("customer"),
                col("merchant"),
                lit(ist_str).alias("detectionTime")
        )
        logger.info(f"PatId1 detection complete. Matches found: {patid1_detections.count()}")

        return patid1_detections
    
    except Exception as e:
        logger.error(f"Error in detect_patid1: {str(e)}")
        raise

# COMMAND ----------

def detect_patid2(df,
      txn_threshold: int = 80,
      avg_value_threshold: float = 23.0,
      pattern_id: str = "PatId2",
      action_type: str = "CHILD"):
 """
 Detects customers for CHILD action based on PatId2 pattern:
 - Customers with >= `txn_threshold` transactions
 - AND average transaction value < `avg_value_threshold`
 """
 try:
  logger.info("Starting PatId2 detection")

  # Step 1: Aggregate transaction count and average value per customer per merchant
  logger.info("Aggregating customer transaction stats per merchant")
  patid2_stats = df.groupBy("merchant", "customer") \
   .agg(
    count("*").alias("txn_count"),
    avg("amount").alias("avg_value")
   )

  # Get current IST time
  ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
  ist_str = ist_now.strftime("%Y-%m-%d %H:%M:%S")

  # Step 2: Apply pattern condition
  logger.info(f"Filtering customers with txn_count >= {txn_threshold} and avg_value < {avg_value_threshold}")
  patid2_detections = patid2_stats \
   .filter((col("txn_count") >= txn_threshold) & (col("avg_value") < avg_value_threshold)) \
   .select(
    lit(pattern_id).alias("patternId"),
    lit(action_type).alias("ActionType"),
    col("customer"),
    col("merchant"),
    lit(ist_str).alias("detectionTime")
   )

  logger.info(f"PatId2 detection complete. Matches found: {patid2_detections.count()}")
  return patid2_detections

 except Exception as e:
  logger.error(f"Error in detect_patid2: {str(e)}")
  raise

# COMMAND ----------


def detect_patid3(df,
      min_female_threshold: int = 100,
      pattern_id: str = "PatId3",
      action_type: str = "DEI-NEEDED"):
 """
 Detects merchants that are DEI-NEEDED:
 - Female customer count > `min_female_threshold`
 - Female customer count < Male customer count
 """
 try:
  logger.info("Starting PatId3 detection")

  # Step 1: Count distinct male and female customers per merchant
  logger.info("Counting distinct male and female customers per merchant")
  gender_counts = df.groupBy("merchant", "gender") \
   .agg(countDistinct("customer").alias("customer_count"))

  # Step 2: Pivot gender into columns
  logger.info("Pivoting gender counts")
  gender_pivot = gender_counts.groupBy("merchant") \
   .pivot("gender", ["M", "F"]) \
   .sum("customer_count") \
   .fillna(0)


  # Get current IST time
  ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
  ist_str = ist_now.strftime("%Y-%m-%d %H:%M:%S")

  # Step 3: Apply DEI pattern logic
  logger.info("Filtering merchants that meet DEI criteria")
  patid3_detections = gender_pivot \
   .filter((col("F") > min_female_threshold) & (col("F") < col("M"))) \
   .select(
    lit(pattern_id).alias("patternId"),
    lit(action_type).alias("ActionType"),
    lit("").alias("customer"),
    col("merchant"),
    lit(ist_str).alias("detectionTime")
   )

  logger.info(f"PatId3 detection complete. Matches found: {patid3_detections.count()}")
  return patid3_detections

 except Exception as e:
  logger.error(f"Error in detect_patid3: {str(e)}")
  raise


# COMMAND ----------

# if it is over time we will use structured streaming with watermarking

# COMMAND ----------

# Paths and config
input_path = "abfss://silver@devdolphinssa.dfs.core.windows.net/transactions/"
output_path = "abfss://gold@devdolphinssa.dfs.core.windows.net/detections"
detection_table = "default.detection_results_table"
export_chunk_size = 50

# Track last version
last_version = None

silver_delta = DeltaTable.forPath(spark, input_path)
while True:
    try:
        current_version = silver_delta.history(1).select("version").collect()[0]["version"]

        if last_version is None :
            last_version = current_version
            logger.info(f"Initial version {last_version}. Waiting for updates.")
        elif current_version > last_version:
            logger.info(f"New data found. Version changed from {last_version} to {current_version}")
            last_version = current_version
            # Re-load ALL silver data every time
            full_df = spark.read.format("delta").load(input_path)

            required_cols = {"merchant", "customer", "amount", "gender"}
            if not required_cols.issubset(set(full_df.columns)):
                missing = required_cols - set(full_df.columns)
                raise ValueError(f"Missing required columns: {missing}")

                # -------------------------------
                # Pattern Detection
                # -------------------------------
            logger.info("Running pattern detection")
            
            start_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
            start_time_str = start_time.strftime("%Y-%m-%d_%H:%M:%S")

            d1 = detect_patid1(full_df).withColumn("startTime", lit(start_time_str))
            d2 = detect_patid2(full_df).withColumn("startTime", lit(start_time_str))
            d3 = detect_patid3(full_df).withColumn("startTime", lit(start_time_str))

            detections = d1.unionByName(d2).unionByName(d3).persist()

            if not detections.rdd.isEmpty():

                # Define a deterministic order (adjust columns as needed)
                window_spec = Window.orderBy("detectionTime")  # or any column you trust for ordering
                # Add row number
                detections = detections.withColumn("row_num", row_number().over(window_spec))

                # Create chunk_id
                detections = detections.withColumn("chunk_id", floor((col("row_num") - 1) / export_chunk_size)).drop("row_num").repartition("chunk_id")

                # Get list of chunk IDs
                chunk_ids = [row["chunk_id"] for row in detections.select("chunk_id").distinct().collect()]

                # -------------------------------
                # Append to Delta Table
                # -------------------------------
                for chunk_id in chunk_ids:
                    chunk_df = detections.filter(col("chunk_id") == chunk_id).drop("chunk_id")
                    if chunk_df.count() == export_chunk_size:
                        unique_path = f"{output_path}/detections_{start_time_str}_{chunk_id}.parquet"
                        chunk_df.write.mode("overwrite").parquet(unique_path)
                        print(f"Wrote chunk {chunk_id} to {unique_path}")
                    else:
                        # Save smaller chunk (e.g., last chunk) to SQL instead
                        print(f"Chunk {chunk_id} has less than {export_chunk_size} records — saving to SQL")
                        chunk_df.write.mode("append").saveAsTable(detection_table)

                # -------------------------------
                # Export if threshold met
                # -------------------------------
                while True:
                    existing_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {detection_table}").collect()[0]["cnt"]
                    
                    if existing_count < export_chunk_size:
                        logger.info(f"Fewer than {export_chunk_size} records remain. Exiting export loop.")
                        break
                    logger.info(f"Exporting {export_chunk_size} records to ADLS")

                    to_export = spark.sql(f"""
                        SELECT * FROM (
                            SELECT *, ROW_NUMBER() OVER (ORDER BY detectionTime ASC) AS rn FROM {detection_table} )
                        WHERE rn <= {export_chunk_size}""")

                    unique_file_path = f"{output_path}/detections.parquet"
                    logger.info(f"Writing to: {unique_file_path}")
                    to_export.write.mode("overwrite").parquet(unique_file_path)
                    to_export.select("detectionTime").createOrReplaceTempView("to_delete")

                    spark.sql(f"""
                        DELETE FROM {detection_table}
                        WHERE detectionTime IN (SELECT detectionTime FROM to_delete)
                    """)

                    logger.info(f"One chunk of {export_chunk_size} records exported and deleted from the table")
                    time.sleep(1)  # optional delay to avoid rapid-looping, especially if time.time() creates same value
            else :
                logger.info("No detections found")
        else:
            logger.info("No new data")


        time.sleep(1)
    except Exception as e:
        logger.error(f"Error : {e}")
        raise
    

# COMMAND ----------

