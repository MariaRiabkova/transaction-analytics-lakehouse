from __future__ import annotations

import argparse
import os
import traceback
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    current_timestamp,
    input_file_name,
    lit,
    lower,
    regexp_replace,
    size,
    to_date,
    to_timestamp,
    trim,
    when,
)
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

JOB_NAME = "load_cancellations_silver"
SILVER_TABLE = "local.silver.cancellations_clean"
AUDIT_TABLE = "local.control.file_load_audit"

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "cancellations/day={day}/cancellations.jsonl"
)

# Raw JSON fields are intentionally read as strings.
# This keeps the job stable even if values are null, empty strings, "none", or malformed.
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


def parse_day(day: str):
    return datetime.strptime(day, "%Y-%m-%d").date()


def get_s3a_endpoint() -> str:
    if MINIO_ENDPOINT.startswith(("http://", "https://")):
        return MINIO_ENDPOINT
    return f"http://{MINIO_ENDPOINT}"


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
    spark.sql("CREATE DATABASE IF NOT EXISTS local.silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.control")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            cancellation_id_raw STRING,
            original_transaction_id_raw STRING,
            reason_raw STRING,
            cancelled_at_raw STRING,
            refund_amount_raw STRING,

            cancellation_id BIGINT,
            original_transaction_id BIGINT,
            reason STRING,
            cancelled_at TIMESTAMP,
            cancelled_date DATE,
            refund_amount DECIMAL(18, 2),

            invalid_cancellation_id BOOLEAN,
            invalid_original_transaction_id BOOLEAN,
            invalid_cancelled_at BOOLEAN,
            invalid_refund_amount BOOLEAN,
            negative_refund_amount BOOLEAN,
            missing_reason BOOLEAN,

            is_valid_record BOOLEAN,
            validation_errors ARRAY<STRING>,

            file_date DATE,
            source_file STRING,
            raw_bucket STRING,
            raw_object_path STRING,
            loaded_at TIMESTAMP,
            processed_at TIMESTAMP
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
            SILVER_TABLE,
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
    print(f"Target: {SILVER_TABLE}")

    try:
        raw_df = spark.read.schema(SOURCE_SCHEMA).json(source_path)

        base_df = (
            raw_df
            .select(
                col("cancellation_id").alias("cancellation_id_raw"),
                col("original_transaction_id").alias("original_transaction_id_raw"),
                col("reason").alias("reason_raw"),
                col("cancelled_at").alias("cancelled_at_raw"),
                col("refund_amount").alias("refund_amount_raw"),
            )
            .withColumn("source_file", input_file_name())
            .withColumn("raw_bucket", lit(RAW_BUCKET))
            .withColumn("raw_object_path", lit(raw_object_path))
            .withColumn("file_date", lit(day).cast("date"))
            .withColumn("loaded_at", current_timestamp())
        )

        # Normalize common empty values to simplify validations.
        cleaned_df = (
            base_df
            .withColumn("cancellation_id_clean", lower(trim(col("cancellation_id_raw"))))
            .withColumn(
                "original_transaction_id_clean",
                lower(trim(col("original_transaction_id_raw"))),
            )
            .withColumn("reason_clean", lower(trim(col("reason_raw"))))
            .withColumn("cancelled_at_clean", trim(col("cancelled_at_raw")))
            .withColumn("refund_amount_clean", lower(trim(col("refund_amount_raw"))))
        )

        empty_values = ["", "none", "null", "nan"]

        parsed_df = (
            cleaned_df
            .withColumn(
                "cancellation_id",
                when(
                    col("cancellation_id_clean").isin(empty_values),
                    None,
                ).otherwise(col("cancellation_id_clean").cast("bigint")),
            )
            .withColumn(
                "original_transaction_id",
                when(
                    col("original_transaction_id_clean").isin(empty_values),
                    None,
                ).otherwise(col("original_transaction_id_clean").cast("bigint")),
            )
            .withColumn(
                "reason",
                when(col("reason_clean").isin(empty_values), None)
                .otherwise(col("reason_clean")),
            )
            .withColumn(
                "cancelled_at",
                to_timestamp(col("cancelled_at_clean"), "yyyy MMM dd HH:mm"),
            )
            .withColumn(
                "refund_amount",
                when(
                    col("refund_amount_clean").isin(empty_values),
                    None,
                ).otherwise(
                    regexp_replace(col("refund_amount_clean"), ",", ".").cast("decimal(18,2)")
                ),
            )
        )

        quality_df = (
            parsed_df
            .withColumn(
                "invalid_cancellation_id",
                col("cancellation_id_raw").isNotNull()
                & (~col("cancellation_id_clean").isin(empty_values))
                & col("cancellation_id").isNull(),
            )
            .withColumn(
                "invalid_original_transaction_id",
                col("original_transaction_id_raw").isNotNull()
                & (~col("original_transaction_id_clean").isin(empty_values))
                & col("original_transaction_id").isNull(),
            )
            .withColumn(
                "invalid_cancelled_at",
                col("cancelled_at_raw").isNotNull()
                & (trim(col("cancelled_at_raw")) != "")
                & col("cancelled_at").isNull(),
            )
            .withColumn(
                "invalid_refund_amount",
                col("refund_amount_raw").isNotNull()
                & (~col("refund_amount_clean").isin(empty_values))
                & col("refund_amount").isNull(),
            )
            .withColumn(
                "negative_refund_amount",
                col("refund_amount").isNotNull() & (col("refund_amount") < 0),
            )
            .withColumn(
                "missing_reason",
                col("reason").isNull(),
            )
            .withColumn("cancelled_date", to_date(col("cancelled_at")))
            .withColumn(
                "is_valid_record",
                ~(
                    col("invalid_cancellation_id")
                    | col("invalid_original_transaction_id")
                    | col("invalid_cancelled_at")
                    | col("invalid_refund_amount")
                    | col("missing_reason")
                ),
            )
            .withColumn(
                "validation_errors",
                array(
                    when(col("invalid_cancellation_id"), lit("invalid_cancellation_id")),
                    when(
                        col("invalid_original_transaction_id"),
                        lit("invalid_original_transaction_id"),
                    ),
                    when(col("invalid_cancelled_at"), lit("invalid_cancelled_at")),
                    when(col("invalid_refund_amount"), lit("invalid_refund_amount")),
                    when(col("negative_refund_amount"), lit("negative_refund_amount")),
                    when(col("missing_reason"), lit("missing_reason")),
                ),
            )
            .withColumn(
                "validation_errors",
                # remove nulls from array
                regexp_replace(
                    col("validation_errors").cast("string"),
                    r"(^\\[null,?\\s*|,?\\s*null\\])",
                    "",
                ),
            )
        )

        # Rebuild validation_errors properly as ARRAY<STRING> using SQL expression.
        quality_df.createOrReplaceTempView("cancellations_quality_tmp")
        final_df = spark.sql(
            """
            SELECT
                cancellation_id_raw,
                original_transaction_id_raw,
                reason_raw,
                cancelled_at_raw,
                refund_amount_raw,

                cancellation_id,
                original_transaction_id,
                reason,
                cancelled_at,
                cancelled_date,
                refund_amount,

                invalid_cancellation_id,
                invalid_original_transaction_id,
                invalid_cancelled_at,
                invalid_refund_amount,
                negative_refund_amount,
                missing_reason,

                is_valid_record,
                filter(
                    array(
                        IF(invalid_cancellation_id, 'invalid_cancellation_id', NULL),
                        IF(invalid_original_transaction_id, 'invalid_original_transaction_id', NULL),
                        IF(invalid_cancelled_at, 'invalid_cancelled_at', NULL),
                        IF(invalid_refund_amount, 'invalid_refund_amount', NULL),
                        IF(negative_refund_amount, 'negative_refund_amount', NULL),
                        IF(missing_reason, 'missing_reason', NULL)
                    ),
                    x -> x IS NOT NULL
                ) AS validation_errors,

                file_date,
                source_file,
                raw_bucket,
                raw_object_path,
                loaded_at,
                current_timestamp() AS processed_at
            FROM cancellations_quality_tmp
            """
        )

        rows_read = final_df.count()

        print(f"Rows: {rows_read}")
        final_df.printSchema()

        (
            final_df.writeTo(SILVER_TABLE)
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
            rows_read=rows_read,
            rows_written=rows_read,
            started_at=started_at,
            finished_at=finished_at,
        )

        print(f"Saved to {SILVER_TABLE}")
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
