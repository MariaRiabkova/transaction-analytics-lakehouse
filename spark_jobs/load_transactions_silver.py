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
    expr,
    from_unixtime,
    input_file_name,
    lit,
    lower,
    to_date,
    trim,
    upper,
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

JOB_NAME = "load_transactions_silver"
SILVER_TABLE = "local.silver.transactions_clean"
AUDIT_TABLE = "local.control.file_load_audit"

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "day={day}/slot={slot}/transactions.jsonl"
)

SOURCE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("user_uuid", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("transaction_type", StringType(), nullable=True),
        StructField("promo_code_id", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
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
            transaction_id_raw STRING,
            user_id_raw STRING,
            user_uuid_raw STRING,
            amount_raw STRING,
            currency_raw STRING,
            transaction_type_raw STRING,
            promo_code_id_raw STRING,
            status_raw STRING,
            created_at_raw STRING,

            transaction_id BIGINT,
            user_id BIGINT,
            user_uuid STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            transaction_type STRING,
            promo_code_id BIGINT,
            status STRING,
            created_at TIMESTAMP,
            created_date DATE,

            missing_transaction_id BOOLEAN,
            invalid_transaction_id BOOLEAN,
            missing_user_id BOOLEAN,
            invalid_user_id BOOLEAN,
            missing_user_uuid BOOLEAN,
            missing_amount BOOLEAN,
            invalid_amount BOOLEAN,
            negative_amount BOOLEAN,
            zero_amount BOOLEAN,
            missing_currency BOOLEAN,
            unsupported_currency BOOLEAN,
            missing_transaction_type BOOLEAN,
            unsupported_transaction_type BOOLEAN,
            missing_status BOOLEAN,
            missing_created_at BOOLEAN,
            invalid_created_at BOOLEAN,
            invalid_promo_code_id BOOLEAN,

            is_valid_record BOOLEAN,
            validation_errors ARRAY<STRING>,

            file_date DATE,
            slot STRING,
            source_file STRING,
            raw_bucket STRING,
            raw_object_path STRING,
            loaded_at TIMESTAMP,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (file_date, slot)
        """
    )


def write_audit_event(
    spark: SparkSession,
    *,
    run_id: str,
    day: str,
    slot: str,
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
            "transactions",
            source_url,
            RAW_BUCKET,
            raw_object_path,
            SILVER_TABLE,
            parse_day(day),
            slot,
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


def load_transactions_for_slot(
    spark: SparkSession,
    *,
    day: str,
    slot: str,
    run_id: str,
) -> None:
    raw_object_path = f"transactions/day={day}/slot={slot}/transactions.jsonl"
    source_path = f"s3a://{RAW_BUCKET}/{raw_object_path}"
    source_url = SOURCE_URL_TEMPLATE.format(day=day, slot=slot)
    started_at = utc_now_naive()

    print(f"Run id: {run_id}")
    print(f"Source URL: {source_url}")
    print(f"Source path: {source_path}")
    print(f"Target: {SILVER_TABLE}")

    try:
        raw_df = spark.read.schema(SOURCE_SCHEMA).json(source_path)

        base_df = (
            raw_df
            .select(
                col("transaction_id").alias("transaction_id_raw"),
                col("user_id").alias("user_id_raw"),
                col("user_uuid").alias("user_uuid_raw"),
                col("amount").alias("amount_raw"),
                col("currency").alias("currency_raw"),
                col("transaction_type").alias("transaction_type_raw"),
                col("promo_code_id").alias("promo_code_id_raw"),
                col("status").alias("status_raw"),
                col("created_at").alias("created_at_raw"),
            )
            .withColumn("source_file", input_file_name())
            .withColumn("raw_bucket", lit(RAW_BUCKET))
            .withColumn("raw_object_path", lit(raw_object_path))
            .withColumn("file_date", lit(day).cast("date"))
            .withColumn("slot", lit(slot))
            .withColumn("loaded_at", current_timestamp())
        )

        empty_values = ["", "none", "null", "nan"]

        parsed_df = (
            base_df
            .withColumn("transaction_id_clean", lower(trim(col("transaction_id_raw"))))
            .withColumn("user_id_clean", lower(trim(col("user_id_raw"))))
            .withColumn("user_uuid_clean", trim(col("user_uuid_raw")))
            .withColumn("amount_clean", lower(trim(col("amount_raw"))))
            .withColumn("currency_clean", upper(trim(col("currency_raw"))))
            .withColumn("transaction_type_clean", lower(trim(col("transaction_type_raw"))))
            .withColumn("promo_code_id_clean", lower(trim(col("promo_code_id_raw"))))
            .withColumn("status_clean", lower(trim(col("status_raw"))))
            .withColumn("created_at_clean", lower(trim(col("created_at_raw"))))
            .withColumn(
                "transaction_id",
                when(col("transaction_id_clean").isin(empty_values), None)
                .otherwise(expr("try_cast(transaction_id_clean AS BIGINT)")),
            )
            .withColumn(
                "user_id",
                when(col("user_id_clean").isin(empty_values), None)
                .otherwise(expr("try_cast(user_id_clean AS BIGINT)")),
            )
            .withColumn(
                "user_uuid",
                when(
                    col("user_uuid_raw").isNull()
                    | lower(trim(col("user_uuid_raw"))).isin(empty_values),
                    None,
                ).otherwise(col("user_uuid_clean")),
            )
            .withColumn(
                "amount",
                when(col("amount_clean").isin(empty_values), None)
                .otherwise(
                    expr(
                        "try_cast(regexp_replace(amount_clean, ',', '.') AS DECIMAL(18,2))"
                    )
                ),
            )
            .withColumn(
                "currency",
                when(lower(col("currency_clean")).isin(empty_values), None)
                .otherwise(col("currency_clean")),
            )
            .withColumn(
                "transaction_type",
                when(col("transaction_type_clean").isin(empty_values), None)
                .otherwise(col("transaction_type_clean")),
            )
            .withColumn(
                "promo_code_id",
                when(col("promo_code_id_clean").isin(empty_values), None)
                .otherwise(expr("try_cast(promo_code_id_clean AS BIGINT)")),
            )
            .withColumn(
                "status",
                when(col("status_clean").isin(empty_values), None)
                .otherwise(col("status_clean")),
            )
            .withColumn(
                "created_at_bigint",
                when(col("created_at_clean").isin(empty_values), None)
                .otherwise(expr("try_cast(created_at_clean AS BIGINT)")),
            )
            .withColumn(
                "created_at",
                when(col("created_at_bigint").isNull(), None)
                .otherwise(from_unixtime(col("created_at_bigint")).cast("timestamp")),
            )
            .withColumn("created_date", to_date(col("created_at")))
        )

        quality_df = (
            parsed_df
            .withColumn(
                "missing_transaction_id",
                col("transaction_id_raw").isNull()
                | col("transaction_id_clean").isin(empty_values),
            )
            .withColumn(
                "invalid_transaction_id",
                col("transaction_id_raw").isNotNull()
                & (~col("transaction_id_clean").isin(empty_values))
                & col("transaction_id").isNull(),
            )
            .withColumn(
                "missing_user_id",
                col("user_id_raw").isNull() | col("user_id_clean").isin(empty_values),
            )
            .withColumn(
                "invalid_user_id",
                col("user_id_raw").isNotNull()
                & (~col("user_id_clean").isin(empty_values))
                & col("user_id").isNull(),
            )
            .withColumn("missing_user_uuid", col("user_uuid").isNull())
            .withColumn(
                "missing_amount",
                col("amount_raw").isNull() | col("amount_clean").isin(empty_values),
            )
            .withColumn(
                "invalid_amount",
                col("amount_raw").isNotNull()
                & (~col("amount_clean").isin(empty_values))
                & col("amount").isNull(),
            )
            .withColumn("negative_amount", col("amount").isNotNull() & (col("amount") < 0))
            .withColumn("zero_amount", col("amount").isNotNull() & (col("amount") == 0))
            .withColumn("missing_currency", col("currency").isNull())
            .withColumn(
                "unsupported_currency",
                col("currency").isNotNull() & (~col("currency").isin("TGRK", "PUNK", "RUB")),
            )
            .withColumn("missing_transaction_type", col("transaction_type").isNull())
            .withColumn(
                "unsupported_transaction_type",
                col("transaction_type").isNotNull()
                & (~col("transaction_type").isin("purchase", "transfer", "refund")),
            )
            .withColumn("missing_status", col("status").isNull())
            .withColumn(
                "missing_created_at",
                col("created_at_raw").isNull() | col("created_at_clean").isin(empty_values),
            )
            .withColumn(
                "invalid_created_at",
                col("created_at_raw").isNotNull()
                & (~col("created_at_clean").isin(empty_values))
                & col("created_at").isNull(),
            )
            .withColumn(
                "invalid_promo_code_id",
                col("promo_code_id_raw").isNotNull()
                & (~col("promo_code_id_clean").isin(empty_values))
                & col("promo_code_id").isNull(),
            )
            .withColumn(
                "is_valid_record",
                ~(
                    col("missing_transaction_id")
                    | col("invalid_transaction_id")
                    | col("invalid_user_id")
                    | col("missing_amount")
                    | col("invalid_amount")
                    | col("missing_currency")
                    | col("unsupported_currency")
                    | col("missing_transaction_type")
                    | col("unsupported_transaction_type")
                    | col("missing_status")
                    | col("missing_created_at")
                    | col("invalid_created_at")
                    | col("invalid_promo_code_id")
                ),
            )
        )

        quality_df.createOrReplaceTempView("transactions_quality_tmp")

        final_df = spark.sql(
            """
            SELECT
                transaction_id_raw,
                user_id_raw,
                user_uuid_raw,
                amount_raw,
                currency_raw,
                transaction_type_raw,
                promo_code_id_raw,
                status_raw,
                created_at_raw,

                transaction_id,
                user_id,
                user_uuid,
                amount,
                currency,
                transaction_type,
                promo_code_id,
                status,
                created_at,
                created_date,

                missing_transaction_id,
                invalid_transaction_id,
                missing_user_id,
                invalid_user_id,
                missing_user_uuid,
                missing_amount,
                invalid_amount,
                negative_amount,
                zero_amount,
                missing_currency,
                unsupported_currency,
                missing_transaction_type,
                unsupported_transaction_type,
                missing_status,
                missing_created_at,
                invalid_created_at,
                invalid_promo_code_id,

                is_valid_record,
                filter(
                    array(
                        IF(missing_transaction_id, 'missing_transaction_id', NULL),
                        IF(invalid_transaction_id, 'invalid_transaction_id', NULL),
                        IF(missing_user_id, 'missing_user_id', NULL),
                        IF(invalid_user_id, 'invalid_user_id', NULL),
                        IF(missing_user_uuid, 'missing_user_uuid', NULL),
                        IF(missing_amount, 'missing_amount', NULL),
                        IF(invalid_amount, 'invalid_amount', NULL),
                        IF(negative_amount, 'negative_amount', NULL),
                        IF(zero_amount, 'zero_amount', NULL),
                        IF(missing_currency, 'missing_currency', NULL),
                        IF(unsupported_currency, 'unsupported_currency', NULL),
                        IF(missing_transaction_type, 'missing_transaction_type', NULL),
                        IF(unsupported_transaction_type, 'unsupported_transaction_type', NULL),
                        IF(missing_status, 'missing_status', NULL),
                        IF(missing_created_at, 'missing_created_at', NULL),
                        IF(invalid_created_at, 'invalid_created_at', NULL),
                        IF(invalid_promo_code_id, 'invalid_promo_code_id', NULL)
                    ),
                    x -> x IS NOT NULL
                ) AS validation_errors,

                file_date,
                slot,
                source_file,
                raw_bucket,
                raw_object_path,
                loaded_at,
                current_timestamp() AS processed_at
            FROM transactions_quality_tmp
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
            slot=slot,
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
                slot=slot,
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
    parser.add_argument("--slot", required=True, help="Slot in HH-MM format")
    args = parser.parse_args()

    run_id = str(uuid4())

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    ensure_tables(spark)
    load_transactions_for_slot(
        spark,
        day=args.day,
        slot=args.slot,
        run_id=run_id,
    )

    spark.stop()


if __name__ == "__main__":
    main()
