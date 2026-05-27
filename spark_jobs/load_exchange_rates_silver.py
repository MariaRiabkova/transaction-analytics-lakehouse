from __future__ import annotations

import argparse
import os
import traceback
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_unixtime,
    input_file_name,
    lit,
    lower,
    to_date,
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

JOB_NAME = "load_exchange_rates_silver"
SILVER_TABLE = "local.silver.exchange_rates_clean"
AUDIT_TABLE = "local.control.file_load_audit"

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "exchange_rates/day={day}/rates.jsonl"
)

# Читаем всё как строки, чтобы не падать на грязных значениях.
SOURCE_SCHEMA = StructType(
    [
        StructField("update_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("rate_tgrk_punk", StringType(), nullable=True),
        StructField("rate_tgrk_rub", StringType(), nullable=True),
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
            update_id_raw STRING,
            timestamp_raw STRING,
            rate_tgrk_punk_raw STRING,
            rate_tgrk_rub_raw STRING,

            update_id BIGINT,
            rate_timestamp TIMESTAMP,
            rate_date DATE,
            rate_tgrk_punk DOUBLE,
            rate_tgrk_rub DOUBLE,

            missing_update_id BOOLEAN,
            missing_timestamp BOOLEAN,
            invalid_update_id BOOLEAN,
            invalid_timestamp BOOLEAN,
            invalid_rate_tgrk_punk BOOLEAN,
            invalid_rate_tgrk_rub BOOLEAN,
            missing_rate_tgrk_punk BOOLEAN,
            missing_rate_tgrk_rub BOOLEAN,
            non_positive_rate_tgrk_punk BOOLEAN,
            non_positive_rate_tgrk_rub BOOLEAN,

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
            "exchange_rates",
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


def load_rates_for_day(spark: SparkSession, day: str, run_id: str) -> None:
    raw_object_path = f"exchange_rates/day={day}/rates.jsonl"
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
                col("update_id").alias("update_id_raw"),
                col("timestamp").alias("timestamp_raw"),
                col("rate_tgrk_punk").alias("rate_tgrk_punk_raw"),
                col("rate_tgrk_rub").alias("rate_tgrk_rub_raw"),
            )
            .withColumn("source_file", input_file_name())
            .withColumn("raw_bucket", lit(RAW_BUCKET))
            .withColumn("raw_object_path", lit(raw_object_path))
            .withColumn("file_date", lit(day).cast("date"))
            .withColumn("loaded_at", current_timestamp())
        )

        empty_values = ["", "none", "null", "nan"]

        parsed_df = (
            base_df
            .withColumn("update_id_clean", lower(trim(col("update_id_raw"))))
            .withColumn("timestamp_clean", lower(trim(col("timestamp_raw"))))
            .withColumn("rate_tgrk_punk_clean", lower(trim(col("rate_tgrk_punk_raw"))))
            .withColumn("rate_tgrk_rub_clean", lower(trim(col("rate_tgrk_rub_raw"))))
            .withColumn(
                "update_id",
                when(col("update_id_clean").isin(empty_values), None)
                .otherwise(col("update_id_clean").cast("bigint")),
            )
            .withColumn(
                "timestamp_bigint",
                when(col("timestamp_clean").isin(empty_values), None)
                .otherwise(col("timestamp_clean").cast("bigint")),
            )
            .withColumn(
                "rate_timestamp",
                when(col("timestamp_bigint").isNull(), None)
                .otherwise(from_unixtime(col("timestamp_bigint")).cast("timestamp")),
            )
            .withColumn("rate_date", to_date(col("rate_timestamp")))
            .withColumn(
                "rate_tgrk_punk",
                when(col("rate_tgrk_punk_clean").isin(empty_values), None)
                .otherwise(col("rate_tgrk_punk_clean").cast("double")),
            )
            .withColumn(
                "rate_tgrk_rub",
                when(col("rate_tgrk_rub_clean").isin(empty_values), None)
                .otherwise(col("rate_tgrk_rub_clean").cast("double")),
            )
        )

        quality_df = (
            parsed_df
            .withColumn(
                "missing_update_id",
                col("update_id_raw").isNull()
                | col("update_id_clean").isin(empty_values),
            )
            .withColumn(
                "missing_timestamp",
                col("timestamp_raw").isNull()
                | col("timestamp_clean").isin(empty_values),
            )
            .withColumn(
                "invalid_update_id",
                col("update_id_raw").isNotNull()
                & (~col("update_id_clean").isin(empty_values))
                & col("update_id").isNull(),
            )
            .withColumn(
                "invalid_timestamp",
                col("timestamp_raw").isNotNull()
                & (~col("timestamp_clean").isin(empty_values))
                & col("rate_timestamp").isNull(),
            )
            .withColumn(
                "invalid_rate_tgrk_punk",
                col("rate_tgrk_punk_raw").isNotNull()
                & (~col("rate_tgrk_punk_clean").isin(empty_values))
                & col("rate_tgrk_punk").isNull(),
            )
            .withColumn(
                "invalid_rate_tgrk_rub",
                col("rate_tgrk_rub_raw").isNotNull()
                & (~col("rate_tgrk_rub_clean").isin(empty_values))
                & col("rate_tgrk_rub").isNull(),
            )
            .withColumn("missing_rate_tgrk_punk", col("rate_tgrk_punk").isNull())
            .withColumn("missing_rate_tgrk_rub", col("rate_tgrk_rub").isNull())
            .withColumn(
                "non_positive_rate_tgrk_punk",
                col("rate_tgrk_punk").isNotNull() & (col("rate_tgrk_punk") <= 0),
            )
            .withColumn(
                "non_positive_rate_tgrk_rub",
                col("rate_tgrk_rub").isNotNull() & (col("rate_tgrk_rub") <= 0),
            )
            .withColumn(
                "is_valid_record",
                ~(
                    col("missing_update_id")
                    | col("missing_timestamp")
                    | col("invalid_update_id")
                    | col("invalid_timestamp")
                    | col("invalid_rate_tgrk_punk")
                    | col("invalid_rate_tgrk_rub")
                    | col("missing_rate_tgrk_punk")
                    | col("missing_rate_tgrk_rub")
                    | col("non_positive_rate_tgrk_punk")
                    | col("non_positive_rate_tgrk_rub")
                ),
            )
        )

        quality_df.createOrReplaceTempView("exchange_rates_quality_tmp")

        final_df = spark.sql(
            """
            SELECT
                update_id_raw,
                timestamp_raw,
                rate_tgrk_punk_raw,
                rate_tgrk_rub_raw,

                update_id,
                rate_timestamp,
                rate_date,
                rate_tgrk_punk,
                rate_tgrk_rub,

                missing_update_id,
                missing_timestamp,
                invalid_update_id,
                invalid_timestamp,
                invalid_rate_tgrk_punk,
                invalid_rate_tgrk_rub,
                missing_rate_tgrk_punk,
                missing_rate_tgrk_rub,
                non_positive_rate_tgrk_punk,
                non_positive_rate_tgrk_rub,

                is_valid_record,
                filter(
                    array(
                        IF(missing_update_id, 'missing_update_id', NULL),
                        IF(missing_timestamp, 'missing_timestamp', NULL),
                        IF(invalid_update_id, 'invalid_update_id', NULL),
                        IF(invalid_timestamp, 'invalid_timestamp', NULL),
                        IF(invalid_rate_tgrk_punk, 'invalid_rate_tgrk_punk', NULL),
                        IF(invalid_rate_tgrk_rub, 'invalid_rate_tgrk_rub', NULL),
                        IF(missing_rate_tgrk_punk, 'missing_rate_tgrk_punk', NULL),
                        IF(missing_rate_tgrk_rub, 'missing_rate_tgrk_rub', NULL),
                        IF(non_positive_rate_tgrk_punk, 'non_positive_rate_tgrk_punk', NULL),
                        IF(non_positive_rate_tgrk_rub, 'non_positive_rate_tgrk_rub', NULL)
                    ),
                    x -> x IS NOT NULL
                ) AS validation_errors,

                file_date,
                source_file,
                raw_bucket,
                raw_object_path,
                loaded_at,
                current_timestamp() AS processed_at
            FROM exchange_rates_quality_tmp
            """
        )

        rows_read = final_df.count()

        print(f"Rows: {rows_read}")
        final_df.printSchema()

        final_df.writeTo(SILVER_TABLE).overwritePartitions()

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
    load_rates_for_day(spark, day=args.day, run_id=run_id)

    spark.stop()


if __name__ == "__main__":
    main()
