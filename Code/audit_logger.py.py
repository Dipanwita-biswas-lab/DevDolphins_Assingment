# Databricks notebook source
# audit_logger.py or /Shared/utils/audit_logger

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

def log_audit(
    job_name: str,
    status: str,
    chunk_id: str = "",
    file_path: str = "",
    row_count: int = 0,
    error_message: str = "",
    audit_table: str = "default.audit_log"
) -> None:
    """
    Generic audit logger for both batch and streaming jobs.

    Parameters:
        spark (SparkSession): Active Spark session
        job_name (str): Identifier for the job or notebook (e.g., RAW_2_BRONZE)
        status (str): 'STARTED', 'SUCCESS', 'FAILURE'
        chunk_id (str): Optional unique ID for a chunk (for batch)
        file_path (str): File written or read (if applicable)
        row_count (int): Records processed
        error_message (str): Error message on failure
        audit_table (str): Delta table to write audit logs to
    """
    try:
        schema = StructType([
            StructField("job_name", StringType()),
            StructField("status", StringType()),
            StructField("chunk_id", StringType()),
            StructField("file_path", StringType()),
            StructField("row_count", IntegerType()),
            StructField("error_message", StringType()),
            StructField("inserted_at", TimestampType())
        ])

        data = [(job_name, status, chunk_id, file_path, row_count, error_message, datetime.now())]
        df = spark.createDataFrame(data, schema)
        df.write.format("delta").mode("append").saveAsTable(audit_table)

    except Exception as e:
        print(f"[AUDIT FAILURE] Could not log audit: {e}")
