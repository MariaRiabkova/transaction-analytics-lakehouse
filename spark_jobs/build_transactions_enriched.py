from __future__ import annotations

import argparse
import os
import traceback
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    count,
    current_timestamp,
    lit,
    row_number,
    unix_timestamp,
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
from pyspark.sql.window import Window


WAREHOUSE_PATH = os.environ.get(
    "ICEBERG_WAREHOUSE_PATH",
    "/home/ubuntu/lab8_lakehouse/warehouse",
)

JOB_NAME = "build_transactions_enriched"

TARGET_TABLE = "local.gold.fct_transactions_enriched"
AUDIT_TABLE = "local.control.file_load_audit"

TRANSACTIONS_TABLE = "local.silver.transactions_clean"
CANCELLATIONS_TABLE = "local.silver.cancellations_clean"
RATE_INTERVALS_TABLE = "local.gold.dim_exchange_rate_intervals"

USERS_TABLE = "local.dictionaries.users"
TEST_USERS_TABLE = "local.dictionaries.test_users"
PROMO_CODES_TABLE = "local.dictionaries.promo_codes"


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
            transaction_id BIGINT,
            user_id BIGINT,
            user_uuid STRING,

            amount DECIMAL(18, 2),
            currency STRING,
            transaction_type STRING,
            status STRING,
            promo_code_id BIGINT,
            created_at TIMESTAMP,
            created_date DATE,

            file_date DATE,
            slot STRING,
            source_file STRING,
            raw_bucket STRING,
            raw_object_path STRING,
            transaction_loaded_at TIMESTAMP,
            transaction_processed_at TIMESTAMP,

            is_valid_transaction BOOLEAN,
            transaction_validation_errors ARRAY<STRING>,

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

            is_purchase_type BOOLEAN,
            is_completed_status BOOLEAN,
            is_completed_purchase BOOLEAN,
            is_failed_status BOOLEAN,
            is_refund_type BOOLEAN,
            is_transfer_type BOOLEAN,

            is_user_found BOOLEAN,
            user_is_test_from_users BOOLEAN,
            user_is_test_from_test_users BOOLEAN,
            is_test_user BOOLEAN,
            user_segment STRING,

            promo_code STRING,
            promo_max_uses BIGINT,
            promo_expiry_date DATE,
            has_promo_code BOOLEAN,
            is_promo_found BOOLEAN,
            is_promo_missing_in_dictionary BOOLEAN,
            is_promo_expired BOOLEAN,

            rate_interval_id STRING,
            rate_timestamp TIMESTAMP,
            rate_date DATE,
            rate_tgrk_punk DOUBLE,
            rate_tgrk_rub DOUBLE,
            is_rate_found BOOLEAN,
            is_rate_valid BOOLEAN,
            rate_selection_strategy STRING,
            rate_time_diff_minutes DOUBLE,

            amount_tgrk DECIMAL(18, 2),
            gross_amount_tgrk DECIMAL(18, 2),

            duplicate_count BIGINT,
            duplicate_rank BIGINT,
            is_duplicate_transaction BOOLEAN,
            is_canonical_transaction BOOLEAN,

            is_cancelled BOOLEAN,
            cancellation_id BIGINT,
            cancelled_at TIMESTAMP,
            cancelled_date DATE,
            cancellation_reason STRING,
            refund_amount DECIMAL(18, 2),
            refund_amount_tgrk DECIMAL(18, 2),
            negative_refund_amount BOOLEAN,
            is_valid_cancellation BOOLEAN,
            cancellation_validation_errors ARRAY<STRING>,

            time_to_cancel_minutes DOUBLE,

            net_amount_tgrk DECIMAL(18, 2),

            gold_window_start DATE,
            gold_window_end DATE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (file_date)
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
            "gold_transactions_enriched",
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


def build_canonical_cancellations(spark: SparkSession):
    cancellations = (
        spark.table(CANCELLATIONS_TABLE)
        .where(col("original_transaction_id").isNotNull())
        .select(
            col("cancellation_id"),
            col("original_transaction_id"),
            col("reason").alias("cancellation_reason"),
            col("cancelled_at"),
            col("cancelled_date"),
            col("refund_amount"),
            col("negative_refund_amount"),
            col("is_valid_record").alias("is_valid_cancellation"),
            col("validation_errors").alias("cancellation_validation_errors"),
            col("processed_at").alias("cancellation_processed_at"),
        )
    )

    w = Window.partitionBy("original_transaction_id").orderBy(
        col("cancelled_at").desc_nulls_last(),
        col("cancellation_processed_at").desc_nulls_last(),
        col("cancellation_id").desc_nulls_last(),
    )

    return (
        cancellations
        .withColumn("rn", row_number().over(w))
        .where(col("rn") == 1)
        .drop("rn", "cancellation_processed_at")
    )


def build_transactions_enriched(
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

        transactions = (
            spark.table(TRANSACTIONS_TABLE)
            .where(
                col("file_date").between(
                    lit(start_date).cast("date"),
                    lit(end_date).cast("date"),
                )
            )
            .select(
                col("transaction_id"),
                col("user_id"),
                col("user_uuid"),
                col("amount"),
                col("currency"),
                col("transaction_type"),
                col("status"),
                col("promo_code_id"),
                col("created_at"),
                col("created_date"),
                col("file_date"),
                col("slot"),
                col("source_file"),
                col("raw_bucket"),
                col("raw_object_path"),
                col("loaded_at").alias("transaction_loaded_at"),
                col("processed_at").alias("transaction_processed_at"),
                col("is_valid_record").alias("is_valid_transaction"),
                col("validation_errors").alias("transaction_validation_errors"),
                col("missing_transaction_id"),
                col("invalid_transaction_id"),
                col("missing_user_id"),
                col("invalid_user_id"),
                col("missing_user_uuid"),
                col("missing_amount"),
                col("invalid_amount"),
                col("negative_amount"),
                col("zero_amount"),
                col("missing_currency"),
                col("unsupported_currency"),
                col("missing_transaction_type"),
                col("unsupported_transaction_type"),
                col("missing_status"),
                col("missing_created_at"),
                col("invalid_created_at"),
                col("invalid_promo_code_id"),
            )
        )

        users = (
            spark.table(USERS_TABLE)
            .select(
                col("user_id").alias("dict_user_id"),
                col("is_test_user").alias("dict_user_is_test"),
            )
            .dropDuplicates(["dict_user_id"])
        )

        test_users = (
            spark.table(TEST_USERS_TABLE)
            .select(col("test_user_uuid").alias("dict_test_user_uuid"))
            .dropDuplicates(["dict_test_user_uuid"])
        )

        promo_codes = (
            spark.table(PROMO_CODES_TABLE)
            .select(
                col("promo_code_id").alias("dict_promo_code_id"),
                col("code").alias("dict_promo_code"),
                col("max_uses").alias("dict_promo_max_uses"),
                col("expiry_date").alias("dict_promo_expiry_date"),
            )
            .dropDuplicates(["dict_promo_code_id"])
        )

        rates = (
            spark.table(RATE_INTERVALS_TABLE)
            .select(
                col("rate_interval_id"),
                col("valid_from"),
                col("valid_to"),
                col("rate_timestamp"),
                col("rate_date"),
                col("rate_tgrk_punk"),
                col("rate_tgrk_rub"),
                col("is_rate_found").alias("interval_is_rate_found"),
                col("is_rate_valid").alias("interval_is_rate_valid"),
                col("rate_selection_strategy").alias("interval_rate_selection_strategy"),
            )
        )

        cancellations = build_canonical_cancellations(spark)

        joined = (
            transactions.alias("t")
            .join(users.alias("u"), col("t.user_id") == col("u.dict_user_id"), "left")
            .join(test_users.alias("tu"), col("t.user_uuid") == col("tu.dict_test_user_uuid"), "left")
            .join(promo_codes.alias("p"), col("t.promo_code_id") == col("p.dict_promo_code_id"), "left")
            .join(
                broadcast(rates).alias("r"),
                (
                    (col("t.currency") != lit("TGRK"))
                    & col("t.created_at").isNotNull()
                    & (col("t.created_at") >= col("r.valid_from"))
                    & (
                        col("r.valid_to").isNull()
                        | (col("t.created_at") < col("r.valid_to"))
                    )
                ),
                "left",
            )
            .join(cancellations.alias("c"), col("t.transaction_id") == col("c.original_transaction_id"), "left")
        )

        user_is_test_from_users = coalesce(col("u.dict_user_is_test"), lit(False))
        user_is_test_from_test_users = col("tu.dict_test_user_uuid").isNotNull()
        is_test_user = user_is_test_from_users | user_is_test_from_test_users

        prepared_df = (
            joined
            .select(
                col("t.transaction_id").alias("transaction_id"),
                col("t.user_id").alias("user_id"),
                col("t.user_uuid").alias("user_uuid"),

                col("t.amount").alias("amount"),
                col("t.currency").alias("currency"),
                col("t.transaction_type").alias("transaction_type"),
                col("t.status").alias("status"),
                col("t.promo_code_id").alias("promo_code_id"),
                col("t.created_at").alias("created_at"),
                col("t.created_date").alias("created_date"),

                col("t.file_date").alias("file_date"),
                col("t.slot").alias("slot"),
                col("t.source_file").alias("source_file"),
                col("t.raw_bucket").alias("raw_bucket"),
                col("t.raw_object_path").alias("raw_object_path"),
                col("t.transaction_loaded_at").alias("transaction_loaded_at"),
                col("t.transaction_processed_at").alias("transaction_processed_at"),

                col("t.is_valid_transaction").alias("is_valid_transaction"),
                col("t.transaction_validation_errors").alias("transaction_validation_errors"),

                col("t.missing_transaction_id").alias("missing_transaction_id"),
                col("t.invalid_transaction_id").alias("invalid_transaction_id"),
                col("t.missing_user_id").alias("missing_user_id"),
                col("t.invalid_user_id").alias("invalid_user_id"),
                col("t.missing_user_uuid").alias("missing_user_uuid"),
                col("t.missing_amount").alias("missing_amount"),
                col("t.invalid_amount").alias("invalid_amount"),
                col("t.negative_amount").alias("negative_amount"),
                col("t.zero_amount").alias("zero_amount"),
                col("t.missing_currency").alias("missing_currency"),
                col("t.unsupported_currency").alias("unsupported_currency"),
                col("t.missing_transaction_type").alias("missing_transaction_type"),
                col("t.unsupported_transaction_type").alias("unsupported_transaction_type"),
                col("t.missing_status").alias("missing_status"),
                col("t.missing_created_at").alias("missing_created_at"),
                col("t.invalid_created_at").alias("invalid_created_at"),
                col("t.invalid_promo_code_id").alias("invalid_promo_code_id"),

                (col("t.transaction_type") == "purchase").alias("is_purchase_type"),
                (col("t.status") == "completed").alias("is_completed_status"),
                (
                    (col("t.transaction_type") == "purchase")
                    & (col("t.status") == "completed")
                ).alias("is_completed_purchase"),
                (col("t.status") == "failed").alias("is_failed_status"),
                (col("t.transaction_type") == "refund").alias("is_refund_type"),
                (col("t.transaction_type") == "transfer").alias("is_transfer_type"),

                (
                    col("t.user_id").isNotNull()
                    & col("u.dict_user_id").isNotNull()
                ).alias("is_user_found"),
                user_is_test_from_users.alias("user_is_test_from_users"),
                user_is_test_from_test_users.alias("user_is_test_from_test_users"),
                is_test_user.alias("is_test_user"),
                when(col("t.user_id").isNull(), lit("anonymous"))
                .when(is_test_user, lit("test"))
                .otherwise(lit("real"))
                .alias("user_segment"),

                col("p.dict_promo_code").alias("promo_code"),
                col("p.dict_promo_max_uses").alias("promo_max_uses"),
                col("p.dict_promo_expiry_date").alias("promo_expiry_date"),
                col("t.promo_code_id").isNotNull().alias("has_promo_code"),
                (
                    col("t.promo_code_id").isNull()
                    | col("p.dict_promo_code_id").isNotNull()
                ).alias("is_promo_found"),
                (
                    col("t.promo_code_id").isNotNull()
                    & col("p.dict_promo_code_id").isNull()
                ).alias("is_promo_missing_in_dictionary"),
                (
                    col("t.promo_code_id").isNotNull()
                    & col("p.dict_promo_expiry_date").isNotNull()
                    & (col("t.created_date") > col("p.dict_promo_expiry_date"))
                ).alias("is_promo_expired"),

                col("r.rate_interval_id").alias("rate_interval_id"),
                when(col("t.currency") == "TGRK", None).otherwise(col("r.rate_timestamp")).alias("rate_timestamp"),
                when(col("t.currency") == "TGRK", None).otherwise(col("r.rate_date")).alias("rate_date"),
                when(col("t.currency") == "TGRK", None).otherwise(col("r.rate_tgrk_punk")).alias("rate_tgrk_punk"),
                when(col("t.currency") == "TGRK", None).otherwise(col("r.rate_tgrk_rub")).alias("rate_tgrk_rub"),
                when(col("t.currency") == "TGRK", lit("base_currency"))
                .when(col("r.rate_timestamp").isNotNull(), col("r.interval_rate_selection_strategy"))
                .otherwise(lit("no_rate"))
                .alias("rate_selection_strategy"),

                col("c.cancellation_id").alias("cancellation_id"),
                col("c.cancelled_at").alias("cancelled_at"),
                col("c.cancelled_date").alias("cancelled_date"),
                col("c.cancellation_reason").alias("cancellation_reason"),
                col("c.refund_amount").alias("refund_amount"),
                col("c.negative_refund_amount").alias("negative_refund_amount"),
                col("c.is_valid_cancellation").alias("is_valid_cancellation"),
                col("c.cancellation_validation_errors").alias("cancellation_validation_errors"),
            )
            .withColumn(
                "is_rate_found",
                col("rate_selection_strategy").isin(
                    "base_currency",
                    "asof_previous",
                    "backfill_next",
                ),
            )
            .withColumn(
                "is_rate_valid",
                when(col("currency") == "TGRK", lit(True))
                .otherwise(
                    col("rate_timestamp").isNotNull()
                    & col("rate_tgrk_punk").isNotNull()
                    & col("rate_tgrk_rub").isNotNull()
                    & (col("rate_tgrk_punk") > 0)
                    & (col("rate_tgrk_rub") > 0)
                ),
            )
            .withColumn(
                "rate_time_diff_minutes",
                when(col("rate_timestamp").isNull() | col("created_at").isNull(), None)
                .otherwise(
                    (unix_timestamp(col("rate_timestamp")) - unix_timestamp(col("created_at")))
                    / lit(60.0)
                ),
            )
            .withColumn(
                "amount_tgrk",
                when(col("currency") == "TGRK", col("amount"))
                .when(
                    (col("currency") == "PUNK") & col("rate_tgrk_punk").isNotNull(),
                    col("amount") / col("rate_tgrk_punk"),
                )
                .when(
                    (col("currency") == "RUB") & col("rate_tgrk_rub").isNotNull(),
                    col("amount") / col("rate_tgrk_rub"),
                )
                .otherwise(None)
                .cast("decimal(18,2)"),
            )
            .withColumn("gross_amount_tgrk", col("amount_tgrk"))
            .withColumn("is_cancelled", col("cancellation_id").isNotNull())
            .withColumn(
                "refund_amount_tgrk",
                when(col("cancellation_id").isNull(), lit(0).cast("decimal(18,2)"))
                .when(col("currency") == "TGRK", col("refund_amount"))
                .when(
                    (col("currency") == "PUNK") & col("rate_tgrk_punk").isNotNull(),
                    col("refund_amount") / col("rate_tgrk_punk"),
                )
                .when(
                    (col("currency") == "RUB") & col("rate_tgrk_rub").isNotNull(),
                    col("refund_amount") / col("rate_tgrk_rub"),
                )
                .otherwise(None)
                .cast("decimal(18,2)"),
            )
            .withColumn(
                "time_to_cancel_minutes",
                when(col("cancelled_at").isNull() | col("created_at").isNull(), None)
                .otherwise(
                    (unix_timestamp(col("cancelled_at")) - unix_timestamp(col("created_at")))
                    / lit(60.0)
                ),
            )
            .withColumn(
                "net_amount_tgrk",
                (
                    col("gross_amount_tgrk")
                    - coalesce(col("refund_amount_tgrk"), lit(0).cast("decimal(18,2)"))
                ).cast("decimal(18,2)"),
            )
        )

        duplicate_window = Window.partitionBy("transaction_id")
        duplicate_rank_window = Window.partitionBy("transaction_id").orderBy(
            col("created_at").desc_nulls_last(),
            col("file_date").desc_nulls_last(),
            col("slot").desc_nulls_last(),
            col("transaction_processed_at").desc_nulls_last(),
        )

        final_df = (
            prepared_df
            .withColumn("duplicate_count_tmp", count("*").over(duplicate_window))
            .withColumn("duplicate_rank_tmp", row_number().over(duplicate_rank_window))
            .withColumn(
                "duplicate_count",
                when(col("transaction_id").isNull(), None)
                .otherwise(col("duplicate_count_tmp")),
            )
            .withColumn(
                "duplicate_rank",
                when(col("transaction_id").isNull(), None)
                .otherwise(col("duplicate_rank_tmp")),
            )
            .withColumn(
                "is_duplicate_transaction",
                coalesce(col("duplicate_count") > 1, lit(False)),
            )
            .withColumn(
                "is_canonical_transaction",
                lit(True),
            )
            .withColumn("gold_window_start", lit(start_date).cast("date"))
            .withColumn("gold_window_end", lit(end_date).cast("date"))
            .withColumn("gold_processed_at", current_timestamp())
            .drop("duplicate_count_tmp", "duplicate_rank_tmp")
        )

        rows_read = transactions.count()
        rows_written = final_df.count()

        print(f"Rows read from transactions: {rows_read}")
        print(f"Rows written to gold enriched: {rows_written}")
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
    parser.add_argument("--start-date", required=True, help="Window start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Window end date YYYY-MM-DD")
    args = parser.parse_args()

    run_id = str(uuid4())

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    build_transactions_enriched(
        spark,
        start_date=args.start_date,
        end_date=args.end_date,
        run_id=run_id,
    )

    spark.stop()


if __name__ == "__main__":
    main()
