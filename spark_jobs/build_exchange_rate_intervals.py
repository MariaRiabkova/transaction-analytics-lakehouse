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
    lead,
    lit,
    row_number,
)
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window


WAREHOUSE_PATH = os.environ.get(
    "ICEBERG_WAREHOUSE_PATH",
    "/home/ubuntu/lab8_lakehouse/warehouse",
)

JOB_NAME = "build_exchange_rate_intervals"

SOURCE_TABLE = "local.silver.exchange_rates_clean"
TARGET_TABLE = "local.gold.dim_exchange_rate_intervals"
AUDIT_TABLE = "local.control.file_load_audit"


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
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS local.gold")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.control")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            rate_interval_id STRING,

            valid_from TIMESTAMP,
            valid_to TIMESTAMP,

            valid_from_date DATE,
            valid_to_date DATE,

            rate_update_id BIGINT,
            rate_timestamp TIMESTAMP,
            rate_date DATE,

            rate_tgrk_punk DOUBLE,
            rate_tgrk_rub DOUBLE,

            is_rate_found BOOLEAN,
            is_rate_valid BOOLEAN,
            rate_selection_strategy STRING,

            gold_window_start DATE,
            gold_window_end DATE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (valid_from_date)
        """
    )


def write_audit_event(
    spark: SparkSession,
    *,
    run_id: str,
    start_date: str,
    end_date: str,
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
            "exchange_rate_intervals",
            None,
            None,
            f"gold_window/start={start_date}/end={end_date}",
            TARGET_TABLE,
            parse_day(start_date),
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


def build_exchange_rate_intervals(
    spark: SparkSession,
    *,
    start_date: str,
    end_date: str,
    run_id: str,
) -> None:
    started_at = utc_now_naive()

    print(f"Run id: {run_id}")
    print(f"Window: {start_date} .. {end_date}")
    print(f"Target: {TARGET_TABLE}")

    try:
        ensure_tables(spark)

        valid_rates = (
            spark.table(SOURCE_TABLE)
            .where(col("is_valid_record") == True)
            .where(col("rate_timestamp").isNotNull())
            .where(col("rate_tgrk_punk").isNotNull())
            .where(col("rate_tgrk_rub").isNotNull())
            .where(col("rate_tgrk_punk") > 0)
            .where(col("rate_tgrk_rub") > 0)
            .select(
                col("update_id").alias("rate_update_id"),
                col("rate_timestamp"),
                col("rate_date"),
                col("rate_tgrk_punk"),
                col("rate_tgrk_rub"),
            )
            .dropDuplicates(["rate_timestamp"])
        )

        order_window = Window.orderBy(col("rate_timestamp").asc())

        asof_intervals = (
            valid_rates
            .withColumn("valid_from", col("rate_timestamp"))
            .withColumn("valid_to", lead("rate_timestamp").over(order_window))
            .withColumn("rate_selection_strategy", lit("asof_previous"))
            .withColumn("is_rate_found", lit(True))
            .withColumn("is_rate_valid", lit(True))
        )

        first_rate = (
            valid_rates
            .withColumn("rn", row_number().over(order_window))
            .where(col("rn") == 1)
            .drop("rn")
            .withColumn("valid_from", lit("1900-01-01 00:00:00").cast("timestamp"))
            .withColumn("valid_to", col("rate_timestamp"))
            .withColumn("rate_selection_strategy", lit("backfill_next"))
            .withColumn("is_rate_found", lit(True))
            .withColumn("is_rate_valid", lit(True))
        )

        final_df = (
            first_rate
            .unionByName(asof_intervals)
            .withColumn(
                "rate_interval_id",
                col("valid_from").cast("string"),
            )
            .withColumn("valid_from_date", col("valid_from").cast("date"))
            .withColumn("valid_to_date", col("valid_to").cast("date"))
            .withColumn("gold_window_start", lit(start_date).cast("date"))
            .withColumn("gold_window_end", lit(end_date).cast("date"))
            .withColumn("gold_processed_at", current_timestamp())
            .select(
                "rate_interval_id",
                "valid_from",
                "valid_to",
                "valid_from_date",
                "valid_to_date",
                "rate_update_id",
                "rate_timestamp",
                "rate_date",
                "rate_tgrk_punk",
                "rate_tgrk_rub",
                "is_rate_found",
                "is_rate_valid",
                "rate_selection_strategy",
                "gold_window_start",
                "gold_window_end",
                "gold_processed_at",
            )
        )

        rows_read = valid_rates.count()
        rows_written = final_df.count()

        print(f"Valid rates read: {rows_read}")
        print(f"Intervals written: {rows_written}")
        final_df.printSchema()

        final_df.writeTo(TARGET_TABLE).overwritePartitions()

        finished_at = utc_now_naive()

        write_audit_event(
            spark,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            status="loaded",
            rows_read=rows_read,
            rows_written=rows_written,
            started_at=started_at,
            finished_at=finished_at,
        )

        print(f"Saved to {TARGET_TABLE}")
        print(f"Audit event written to {AUDIT_TABLE}")

    except Exception:
        finished_at = utc_now_naive()
        error_message = traceback.format_exc()

        try:
            write_audit_event(
                spark,
                run_id=run_id,
                start_date=start_date,
                end_date=end_date,
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
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    run_id = str(uuid4())

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    build_exchange_rate_intervals(
        spark,
        start_date=args.start_date,
        end_date=args.end_date,
        run_id=run_id,
    )

    spark.stop()


if __name__ == "__main__":
    main()
