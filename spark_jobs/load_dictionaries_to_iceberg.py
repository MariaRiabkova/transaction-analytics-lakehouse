from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_date
from pyspark.sql.types import (
    BooleanType,
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

AUDIT_TABLE = "local.control.file_load_audit"
JOB_NAME = "load_dictionaries_to_iceberg"

DICTIONARY_TABLES = {
    "users": {
        "path": f"s3a://{RAW_BUCKET}/dictionaries/users.jsonl",
        "raw_object_path": "dictionaries/users.jsonl",
        "source_url": "https://storage.yandexcloud.net/npl-de18-lab8-data/reference/users.jsonl",
        "target_table": "local.dictionaries.users",
        "schema": StructType(
            [
                StructField("user_id", LongType(), nullable=False),
                StructField("user_uuid", StringType(), nullable=False),
                StructField("is_test_user", BooleanType(), nullable=True),
            ]
        ),
    },
    "test_users": {
        "path": f"s3a://{RAW_BUCKET}/dictionaries/test_users.jsonl",
        "raw_object_path": "dictionaries/test_users.jsonl",
        "source_url": "https://storage.yandexcloud.net/npl-de18-lab8-data/reference/test_users.jsonl",
        "target_table": "local.dictionaries.test_users",
        "schema": StructType(
            [
                StructField("test_user_uuid", StringType(), nullable=False),
            ]
        ),
    },
    "promo_codes": {
        "path": f"s3a://{RAW_BUCKET}/dictionaries/promo_codes.jsonl",
        "raw_object_path": "dictionaries/promo_codes.jsonl",
        "source_url": "https://storage.yandexcloud.net/npl-de18-lab8-data/reference/promo_codes.jsonl",
        "target_table": "local.dictionaries.promo_codes",
        "schema": StructType(
            [
                StructField("promo_code_id", LongType(), nullable=False),
                StructField("code", StringType(), nullable=False),
                StructField("max_uses", LongType(), nullable=True),
                StructField("expiry_date", StringType(), nullable=True),
            ]
        ),
    },
}


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


def get_s3a_endpoint() -> str:
    if MINIO_ENDPOINT.startswith("http://") or MINIO_ENDPOINT.startswith("https://"):
        return MINIO_ENDPOINT
    return f"http://{MINIO_ENDPOINT}"


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


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


def write_audit_event(
    spark: SparkSession,
    *,
    run_id: str,
    source_url: str,
    raw_object_path: str,
    target_table: str,
    status: str,
    rows_read: int | None,
    rows_written: int | None,
    file_size_bytes: int | None,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    error_text = error_message
    if error_text and len(error_text) > 4000:
        error_text = error_text[:4000]

    row = [
        (
            run_id,
            JOB_NAME,
            "dictionaries",
            source_url,
            RAW_BUCKET,
            raw_object_path,
            target_table,
            None,  # file_date
            None,  # slot
            status,
            rows_read,
            rows_written,
            file_size_bytes,
            error_text,
            started_at,
            finished_at,
            utc_now_naive(),
        )
    ]

    audit_df = spark.createDataFrame(row, AUDIT_SCHEMA)
    audit_df.writeTo(AUDIT_TABLE).append()


def load_table(
    spark: SparkSession,
    *,
    run_id: str,
    table_name: str,
    config: dict,
) -> int:
    path = config["path"]
    target_table = config["target_table"]
    schema = config["schema"]
    started_at = utc_now_naive()

    print(f"\n=== Loading {table_name} ===")
    print(f"Source: {path}")
    print(f"Target: {target_table}")

    try:
        df = (
            spark.read
            .schema(schema)
            .json(path)
            .withColumn("source_file", input_file_name())
            .withColumn("loaded_at", current_timestamp())
        )

        if table_name == "promo_codes":
            df = df.withColumn("expiry_date", to_date(col("expiry_date"), "yyyy-MM-dd"))

        row_count = df.count()

        print(f"Rows: {row_count}")
        df.printSchema()

        (
            df.writeTo(target_table)
            .using("iceberg")
            .createOrReplace()
        )

        finished_at = utc_now_naive()

        write_audit_event(
            spark,
            run_id=run_id,
            source_url=config["source_url"],
            raw_object_path=config["raw_object_path"],
            target_table=target_table,
            status="loaded",
            rows_read=row_count,
            rows_written=row_count,
            file_size_bytes=None,
            started_at=started_at,
            finished_at=finished_at,
            error_message=None,
        )

        print(f"Saved to {target_table}")
        print(f"Audit event written to {AUDIT_TABLE}")

        return row_count

    except Exception:
        finished_at = utc_now_naive()
        error_message = traceback.format_exc()

        try:
            write_audit_event(
                spark,
                run_id=run_id,
                source_url=config["source_url"],
                raw_object_path=config["raw_object_path"],
                target_table=target_table,
                status="failed",
                rows_read=None,
                rows_written=None,
                file_size_bytes=None,
                started_at=started_at,
                finished_at=finished_at,
                error_message=error_message,
            )
        except Exception as audit_error:
            print(f"Failed to write audit event: {audit_error}")

        raise


def main() -> None:
    run_id = str(uuid4())

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Run id: {run_id}")
    print(f"MinIO endpoint: {get_s3a_endpoint()}")
    print(f"Raw bucket: {RAW_BUCKET}")
    print(f"Iceberg warehouse: {WAREHOUSE_PATH}")

    spark.sql("CREATE DATABASE IF NOT EXISTS local.dictionaries")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.control")

    for table_name, config in DICTIONARY_TABLES.items():
        load_table(
            spark,
            run_id=run_id,
            table_name=table_name,
            config=config,
        )

    print("\n=== Tables in local.dictionaries ===")
    spark.sql("SHOW TABLES IN local.dictionaries").show(truncate=False)

    print("\n=== Counts ===")
    for table_name in DICTIONARY_TABLES:
        spark.sql(
            f"""
            SELECT
                '{table_name}' AS table_name,
                COUNT(*) AS row_count
            FROM local.dictionaries.{table_name}
            """
        ).show(truncate=False)

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
