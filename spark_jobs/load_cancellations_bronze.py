from __future__ import annotations

import argparse
import os
import traceback
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


WAREHOUSE_PATH = os.environ.get(
    "ICEBERG_WAREHOUSE_PATH",
    "/home/ubuntu/lab8_lakehouse/warehouse",
)

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
RAW_BUCKET = os.environ.get("MINIO_RAW_BUCKET", "raw")

JOB_NAME = "load_cancellations_bronze"
BRONZE_TABLE = "local.bronze.cancellations_raw"
AUDIT_TABLE = "local.control.file_load_audit"

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "cancellations/day={day}/cancellations.jsonl"
)

SOURCE_SCHEMA = StructType(
    [
        StructField("cancellation_id", StringType(), nullable=True),
        StructField("original_transaction_id", StringType(), nullable=True),
        StructField("reason", StringType(), nullable=True),
        StructField("cancelled_at", StringType(), nullable=True),
        StructField("refund_amount", StringType(), nullable=True),
    ]
)

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("raw_bucket", StringType(), True),
        StructField("raw_object_path", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("file_date", DateType(), True),
        StructField("slot", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_s3a_endpoint() -> str:
    if MINIO_ENDPOINT.startswith(("http://", "https://")):
        return MINIO_ENDPOINT
    return f"http://{MINIO_ENDPOINT}"


def parse_day(day: str):
    return datetime.strptime(day, "%Y-%m-%d").date()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(JOB_NAME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", get_s3a_endpoint())
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS local.bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.control")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            cancellation_id STRING,
            original_transaction_id STRING,
            reason STRING,
            cancelled_at STRING,
            refund_amount STRING,
            source_file STRING,
            raw_bucket STRING,
            raw_object_path STRING,
            file_date STRING,
            loaded_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (file_date)
        """
    )


def write_audit_event(
    spark: SparkSession,
    *,
    run_id: str,
    day: str,
    source_url: str,
    raw_object_path: str,
    status: str,
    rows_read: int | None,
    rows_written: int | None,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    if error_message and len(error_message) > 4000:
        error_message = error_message[:4000]

    row = [
        (
            run_id,
            JOB_NAME,
            "cancellations",
            source_url,
            RAW_BUCKET,
            raw_object_path,
            BRONZE_TABLE,
            parse_day(day),
            None,
            status,
            rows_read,
            rows_written,
            None,
            error_message,
            started_at,
            finished_at,
            utc_now_naive(),
        )
    ]

    audit_df = spark.createDataFrame(row, AUDIT_SCHEMA)
    audit_df.writeTo(AUDIT_TABLE).append()


def load_cancellations_for_day(spark: SparkSession, day: str, run_id: str) -> None:
    raw_object_path = f"cancellations/day={day}/cancellations.jsonl"
    source_path = f"s3a://{RAW_BUCKET}/{raw_object_path}"
    source_url = SOURCE_URL_TEMPLATE.format(day=day)

    started_at = utc_now_naive()

    print(f"Run id: {run_id}")
    print(f"Source: {source_path}")
    print(f"Target: {BRONZE_TABLE}")

    try:
        df = (
            spark.read
            .schema(SOURCE_SCHEMA)
            .json(source_path)
            .withColumn("source_file", input_file_name())
            .withColumn("raw_bucket", lit(RAW_BUCKET))
            .withColumn("raw_object_path", lit(raw_object_path))
            .withColumn("file_date", lit(day))
            .withColumn("loaded_at", current_timestamp())
        )

        row_count = df.count()

        print(f"Rows: {row_count}")
        df.printSchema()

        (
            df.writeTo(BRONZE_TABLE)
            .overwritePartitions()
        )

        finished_at = utc_now_naive()

        write_audit_event(
            spark,
            run_id=run_id,
            day=day,
            source_url=source_url,
            raw_object_path=raw_object_path,
            status="loaded",
            rows_read=row_count,
            rows_written=row_count,
            started_at=started_at,
            finished_at=finished_at,
        )

        print(f"Saved to {BRONZE_TABLE}")
        print(f"Audit event written to {AUDIT_TABLE}")

    except Exception:
        finished_at = utc_now_naive()
        error_message = traceback.format_exc()

        try:
            write_audit_event(
                spark,
                run_id=run_id,
                day=day,
                source_url=source_url,
                raw_object_path=raw_object_path,
                status="failed",
                rows_read=None,
                rows_written=None,
                started_at=started_at,
                finished_at=finished_at,
                error_message=error_message,
            )
        except Exception as audit_error:
            print(f"Failed to write audit event: {audit_error}")

        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="Day in YYYY-MM-DD format")
    args = parser.parse_args()

    run_id = str(uuid4())

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    ensure_tables(spark)
    load_cancellations_for_day(spark, day=args.day, run_id=run_id)

    print("\n=== Latest audit events ===")
    spark.sql(
        f"""
        SELECT
            run_id,
            job_name,
            raw_object_path,
            target_table,
            status,
            rows_written,
            created_at
        FROM {AUDIT_TABLE}
        WHERE run_id = '{run_id}'
        ORDER BY created_at
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
