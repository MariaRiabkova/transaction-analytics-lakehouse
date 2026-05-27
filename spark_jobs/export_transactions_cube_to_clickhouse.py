from __future__ import annotations

import argparse
import json
import os
import subprocess
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from urllib import parse

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    hour,
    lit,
    sum as spark_sum,
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

JOB_NAME = "export_transactions_cube_to_clickhouse"

SOURCE_TABLE = "local.gold.fct_transactions_enriched"
AUDIT_TABLE = "local.control.file_load_audit"

CH_HOST = os.environ["CLICKHOUSE_HOST"]
CH_PORT = os.environ.get("CLICKHOUSE_PORT", "9000")
CH_HTTP_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
CH_USER = os.environ["CLICKHOUSE_USER"]
CH_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "maria_riabkova")
CH_TABLE = os.environ.get("CLICKHOUSE_TRANSACTIONS_CUBE_TABLE", "transactions_cube_hourly")
CH_FULL_TABLE = f"{CH_DATABASE}.{CH_TABLE}"

TARGET_COLUMNS = [
    "event_date",
    "event_hour",
    "currency",
    "transaction_type",
    "status",
    "user_segment",
    "is_purchase_type",
    "is_completed_status",
    "is_completed_purchase",
    "is_cancelled",
    "cancellation_reason",
    "rate_selection_strategy",
    "transactions_count",
    "canonical_transactions_count",
    "duplicate_transaction_id_rows_count",
    "unique_users_count",
    "gross_amount_tgrk",
    "refund_amount_tgrk",
    "net_amount_tgrk",
    "negative_amount_count",
    "zero_amount_count",
    "cancelled_transactions_count",
]


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


def parse_day(day: str) -> date:
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
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def ch_base_cmd() -> list[str]:
    return [
        "clickhouse-client",
        f"--host={CH_HOST}",
        f"--port={CH_PORT}",
        f"--user={CH_USER}",
        f"--password={CH_PASSWORD}",
    ]


def run_ch_query(query: str) -> str:
    result = subprocess.run(
        [*ch_base_cmd(), f"--query={query}"],
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "ClickHouse query failed\n"
            f"Query: {query}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    return result.stdout.strip()


def json_default(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    return value


def rows_to_json_each_row(rows: list[Any]) -> str:
    lines = []

    for row in rows:
        item = {column: row[column] for column in TARGET_COLUMNS}
        lines.append(json.dumps(item, default=json_default, ensure_ascii=False))

    return "\n".join(lines) + ("\n" if lines else "")


def insert_rows_http_json_each_row(rows: list[Any]) -> None:
    if not rows:
        return

    query = f"""
    INSERT INTO {CH_FULL_TABLE}
    ({", ".join(TARGET_COLUMNS)})
    FORMAT JSONEachRow
    """

    url = f"http://{CH_HOST}:{CH_HTTP_PORT}/?query={parse.quote(query)}"
    headers = {
        "X-ClickHouse-User": CH_USER,
        "X-ClickHouse-Key": CH_PASSWORD,
    }

    response = requests.post(
        url,
        data=rows_to_json_each_row(rows).encode("utf-8"),
        headers=headers,
        timeout=120,
    )

    if response.status_code != 200:
        raise RuntimeError(
            "ClickHouse HTTP insert failed\n"
            f"status_code: {response.status_code}\n"
            f"response: {response.text}"
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
            "clickhouse_transactions_cube_hourly",
            None,
            None,
            f"clickhouse_window/start={start_date}/end={end_date}",
            CH_FULL_TABLE,
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


def build_cube_df(spark: SparkSession, start_date: str, end_date: str):
    source = (
        spark.table(SOURCE_TABLE)
        .where(
            col("file_date").between(
                lit(start_date).cast("date"),
                lit(end_date).cast("date"),
            )
        )
        .where(col("created_date").isNotNull())
        .where(col("created_at").isNotNull())
    )

    cube = (
        source
        .groupBy(
            col("created_date").alias("event_date"),
            hour(col("created_at")).cast("tinyint").alias("event_hour"),
            coalesce(col("currency"), lit("unknown")).alias("currency"),
            coalesce(col("transaction_type"), lit("unknown")).alias("transaction_type"),
            coalesce(col("status"), lit("unknown")).alias("status"),
            coalesce(col("user_segment"), lit("unknown")).alias("user_segment"),
            when(col("is_purchase_type"), lit(1)).otherwise(lit(0)).cast("tinyint").alias("is_purchase_type"),
            when(col("is_completed_status"), lit(1)).otherwise(lit(0)).cast("tinyint").alias("is_completed_status"),
            when(col("is_completed_purchase"), lit(1)).otherwise(lit(0)).cast("tinyint").alias("is_completed_purchase"),
            when(col("is_cancelled"), lit(1)).otherwise(lit(0)).cast("tinyint").alias("is_cancelled"),
            coalesce(col("cancellation_reason"), lit("")).alias("cancellation_reason"),
            coalesce(col("rate_selection_strategy"), lit("unknown")).alias("rate_selection_strategy"),
        )
        .agg(
            count(lit(1)).cast("long").alias("transactions_count"),
            spark_sum(when(col("is_canonical_transaction"), lit(1)).otherwise(lit(0))).cast("long").alias("canonical_transactions_count"),
            spark_sum(when(col("is_duplicate_transaction"), lit(1)).otherwise(lit(0))).cast("long").alias("duplicate_transaction_id_rows_count"),
            countDistinct(col("user_id")).cast("long").alias("unique_users_count"),
            coalesce(spark_sum(col("gross_amount_tgrk")), lit(0).cast("decimal(18,2)")).cast("decimal(18,2)").alias("gross_amount_tgrk"),
            coalesce(spark_sum(col("refund_amount_tgrk")), lit(0).cast("decimal(18,2)")).cast("decimal(18,2)").alias("refund_amount_tgrk"),
            coalesce(spark_sum(col("net_amount_tgrk")), lit(0).cast("decimal(18,2)")).cast("decimal(18,2)").alias("net_amount_tgrk"),
            spark_sum(when(col("negative_amount"), lit(1)).otherwise(lit(0))).cast("long").alias("negative_amount_count"),
            spark_sum(when(col("zero_amount"), lit(1)).otherwise(lit(0))).cast("long").alias("zero_amount_count"),
            spark_sum(when(col("is_cancelled"), lit(1)).otherwise(lit(0))).cast("long").alias("cancelled_transactions_count"),
        )
        .select(*TARGET_COLUMNS)
        .orderBy(
            "event_date",
            "event_hour",
            "currency",
            "transaction_type",
            "status",
            "user_segment",
            "is_cancelled",
            "cancellation_reason",
            "rate_selection_strategy",
        )
    )

    return source, cube


def export_cube_to_clickhouse(
    spark: SparkSession,
    *,
    start_date: str,
    end_date: str,
    run_id: str,
) -> None:
    started_at = utc_now_naive()

    print(f"Run id: {run_id}")
    print(f"Window: {start_date} .. {end_date}")
    print(f"Source: {SOURCE_TABLE}")
    print(f"Target: {CH_FULL_TABLE}")

    rows_read = None
    rows_written = None

    try:
        source_df, cube_df = build_cube_df(spark, start_date, end_date)

        source_df.cache()
        cube_df.cache()

        rows_read = source_df.count()
        cube_rows = cube_df.collect()
        rows_written = len(cube_rows)

        print(f"Rows read from enriched: {rows_read}")
        print(f"Cube rows to insert: {rows_written}")

        delete_query = f"""
        ALTER TABLE {CH_FULL_TABLE}
        DELETE WHERE event_date BETWEEN toDate('{start_date}') AND toDate('{end_date}')
        SETTINGS mutations_sync = 1
        """
        print(f"Deleting old ClickHouse rows for {start_date}..{end_date}")
        run_ch_query(delete_query)

        if cube_rows:
            print("Inserting cube rows into ClickHouse via HTTP JSONEachRow")
            insert_rows_http_json_each_row(cube_rows)
        else:
            print("No cube rows to insert")

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

        print(f"Exported {rows_written} cube rows to {CH_FULL_TABLE}")
        print(f"Audit event written to {AUDIT_TABLE}")

    except Exception as exc:
        finished_at = utc_now_naive()

        try:
            write_audit_event(
                spark,
                run_id=run_id,
                start_date=start_date,
                end_date=end_date,
                status="failed",
                rows_read=rows_read,
                rows_written=rows_written,
                started_at=started_at,
                finished_at=finished_at,
                error_message=str(exc),
            )
        except Exception as audit_exc:
            print(f"Failed to write audit event: {audit_exc}")

        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="Window start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Window end date YYYY-MM-DD")
    args = parser.parse_args()

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        export_cube_to_clickhouse(
            spark,
            start_date=args.start_date,
            end_date=args.end_date,
            run_id=str(uuid.uuid4()),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
