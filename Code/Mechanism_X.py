# Databricks notebook source
# MAGIC %md
# MAGIC Create a mechanism X to
# MAGIC -  invoke every second 
# MAGIC -  create a chunk of next 10,000 transaction entries from GDrive and put them into a ADLS folde

# COMMAND ----------

dbutils.widgets.text("chunk_size", "10000")
dbutils.widgets.text("raw_path", "abfss://raw@devdolphinssa.dfs.core.windows.net/transactions/")
dbutils.widgets.text("adls_raw_path", "https://devdolphinssa.blob.core.windows.net/raw/downloaded_version/transactions.csv")
dbutils.widgets.text("source_url", "https://drive.google.com/uc?export=download&id=1AGXVlDhbMbhoGXDJG0IThnqz86Qy3hqb")

chunk_size = int(dbutils.widgets.get("chunk_size"))
source_url = dbutils.widgets.get("source_url")
raw_path = dbutils.widgets.get("raw_path")
adls_raw_path = dbutils.widgets.get("adls_raw_path")

# COMMAND ----------


import pandas as pd
import logging
import requests
from io import StringIO
from datetime import datetime
from pyspark.sql.types import *
import time
import uuid
from pyspark.sql.utils import AnalysisException



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoogleDriveIngest")


# COMMAND ----------

# MAGIC %run ./audit_logger.py

# COMMAND ----------


def read_csv_from_gdrive(url: str, adls_fallback_path: str= adls_raw_path) -> pd.DataFrame | None:
    """
    Attempts to read a CSV file from a Google Drive public URL.
    If the download yields no data, fallback to loading from ADLS.
    """
    try:
        logger.info(f"Fetching CSV from Google Drive URL: {url}")
        response = requests.get(url)

        if response.status_code != 200:
            error_msg = f"Failed to fetch file. Status code: {response.status_code}"
            logger.error(error_msg)
            log_audit(job_name="GDrive", status="FAILURE", row_count=0, error_message=error_msg)
            logger.warning("GDrive file is empty. Attempting fallback to ADLS...")
            spark_df = (
                spark.read.format("csv")
                .option("header", True)
                .option("inferSchema", True)
                .load(adls_raw_path)
            )
            df = spark_df.toPandas()
            logger.info(f"Fallback read successful. {len(df)} rows loaded from ADLS.")
            log_audit(job_name="ADLS_FALLBACK", status="SUCCESS", row_count=len(df))


        csv_raw = StringIO(response.text)
        df = pd.read_csv(csv_raw , header=0)

        logger.info(f"Successfully read {len(df)} rows from GDrive.")
        log_audit(job_name="GDrive", status="SUCCESS", row_count=len(df))
        if df.empty:
            logger.warning("GDrive file is empty. Attempting fallback to ADLS...")
            spark_df = (
                spark.read.format("csv")
                .option("header", True)
                .option("inferSchema", True)
                .load(adls_raw_path)
            )

            df = spark_df.toPandas()
            logger.info(f"Fallback read successful. {len(df)} rows loaded from ADLS.")
            log_audit(job_name="ADLS_FALLBACK", status="SUCCESS", row_count=len(df))

        return df

    except Exception as e:
        error_msg = f"Exception while reading CSV: {e}"
        logger.error(error_msg, exc_info=True)
        log_audit(job_name="GDrive", status="FAILURE", row_count=0, error_message=str(e))
        return None

    

# COMMAND ----------


def write_chunk_to_adls(spark_df, chunk_id: str) -> str | None:
    """
    Writes the given Spark DataFrame to ADLS in CSV format under a unique chunk ID path.
    Logs audit entry for success or failure.
    """
    try:
        record_count = spark_df.count()
        path = f"{raw_path}chunk_{chunk_id}"

        logger.info(f"Writing chunk {chunk_id} with {record_count} records to: {path}")

        spark_df.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(path)

        logger.info(f"Write successful for chunk {chunk_id}")
        log_audit(job_name = "write_chunk_to_adls",  chunk_id=chunk_id,file_path=path,row_count=record_count,status="SUCCESS")

        return path

    except Exception as e:
        error_msg = f"Write failed for chunk {chunk_id}: {e}"
        logger.error(error_msg, exc_info=True)
        log_audit(job_name = "write_chunk_to_adls",  chunk_id=chunk_id, file_path="", row_count=0, tatus="FAILURE", error_message=str(e) )
        return None


# COMMAND ----------



def process_and_export_csv_chunks(source_url, chunk_size):
    logger.info(f"Starting CSV ingestion from: {source_url}")
    
    # Load CSV to Pandas from GDrive
    pdf = read_csv_from_gdrive(source_url)
    if pdf.empty:
        logger.error("No data found in the source.")
        return

    schema_column = [str(col).strip() for col in pdf.columns]

    total_chunks = (len(pdf) + chunk_size - 1) // chunk_size
    logger.info(f"Total records: {len(pdf)}, Processing in {total_chunks} chunks of {chunk_size} records each")

    for i in range(0, len(pdf), chunk_size):
        chunk_id = str(uuid.uuid4())
        chunk_df = pdf.iloc[i:i + chunk_size]

        try:
            spark_df = spark.createDataFrame(chunk_df, schema=schema_column)
            
            file_path = write_chunk_to_adls(spark_df, chunk_id)

            log_audit( job_name='process_and_export_csv_chunks', chunk_id=chunk_id, file_path=file_path, row_count=len(chunk_df), status="SUCCESS")

        except (ValueError, AnalysisException, Exception) as e:
            logger.error(f"Failed to process chunk {chunk_id}: {e}")
            log_audit(job_name='process_and_export_csv_chunks', chunk_id=chunk_id, file_path="", row_count=len(chunk_df), status="FAILURE", error_message=str(e) )

        # Consider removing sleep for production. Used only to throttle API/DL rate.
        time.sleep(1)

    logger.info("CSV chunk ingestion completed successfully.")



# COMMAND ----------

process_and_export_csv_chunks(source_url, chunk_size)

# COMMAND ----------

