import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg

from transaction_lakehouse.resources.minio import MinioResource
from transaction_lakehouse.utils.minio_io import upload_url_to_minio


PROJECT_DIR = Path("/home/ubuntu/lab8_lakehouse")
RAW_BUCKET = os.environ.get("MINIO_RAW_BUCKET", "raw")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
SPARK_JARS = os.environ["SPARK_JARS"]

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
EXCHANGE_RATES_START_DATE = date(2026, 4, 27)

exchange_rates_partitions_def = dg.DailyPartitionsDefinition(
    start_date=EXCHANGE_RATES_START_DATE.isoformat(),
    end_offset=1,
)

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "exchange_rates/day={day}/rates.jsonl"
)


@dg.asset(
    group_name="exchange_rates",
    partitions_def=exchange_rates_partitions_def,
)
def raw_exchange_rates_file(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    day = context.partition_key
    source_url = SOURCE_URL_TEMPLATE.format(day=day)
    object_name = f"exchange_rates/day={day}/rates.jsonl"

    client = minio.get_client()

    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)

    context.log.info(f"Downloading {source_url}")
    context.log.info(f"Uploading to s3://{RAW_BUCKET}/{object_name}")

    try:
        result = upload_url_to_minio(
            client=client,
            url=source_url,
            bucket_name=RAW_BUCKET,
            object_name=object_name,
        )

        return dg.MaterializeResult(
            metadata={
                "partition_day": day,
                "status": "downloaded",
                "file_exists": True,
                "source_url": source_url,
                "bucket": RAW_BUCKET,
                "object_name": object_name,
                "file_size_bytes": result["file_size_bytes"],
            }
        )

    except FileNotFoundError:
        context.log.warning(f"No exchange rates file for day={day}: {source_url}")

        return dg.MaterializeResult(
            metadata={
                "partition_day": day,
                "status": "no_data",
                "file_exists": False,
                "source_url": source_url,
                "bucket": RAW_BUCKET,
                "object_name": object_name,
            }
        )


@dg.asset(
    group_name="exchange_rates",
    partitions_def=exchange_rates_partitions_def,
    deps=[raw_exchange_rates_file],
)
def silver_exchange_rates_clean(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    day = context.partition_key
    object_name = f"exchange_rates/day={day}/rates.jsonl"

    client = minio.get_client()

    try:
        client.stat_object(RAW_BUCKET, object_name)
        context.log.info(f"Found s3://{RAW_BUCKET}/{object_name}")
    except Exception:
        context.log.warning(
            f"No exchange rates file in MinIO for day={day}; marking partition as no_data."
        )

        return dg.MaterializeResult(
            metadata={
                "partition_day": day,
                "status": "no_data",
                "source_object": object_name,
                "target_table": "local.silver.exchange_rates_clean",
                "rows_written": 0,
            }
        )

    script_path = PROJECT_DIR / "spark_jobs" / "load_exchange_rates_silver.py"

    cmd = [
        SPARK_SUBMIT,
        "--jars",
        SPARK_JARS,
        str(script_path),
        "--day",
        day,
    ]

    context.log.info("Running command: " + " ".join(cmd))

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_DIR),
        text=True,
        capture_output=True,
        env=os.environ.copy(),
    )

    if result.stdout:
        context.log.info(result.stdout)

    if result.stderr:
        context.log.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"spark-submit failed with exit code {result.returncode}"
        )

    return dg.MaterializeResult(
        metadata={
            "partition_day": day,
            "status": "loaded",
            "spark_submit": SPARK_SUBMIT,
            "spark_jars": SPARK_JARS,
            "spark_job": str(script_path),
            "source_object": object_name,
            "target_table": "local.silver.exchange_rates_clean",
            "audit_table": "local.control.file_load_audit",
        }
    )


exchange_rates_daily_job = dg.define_asset_job(
    name="exchange_rates_daily_job",
    selection=dg.AssetSelection.groups("exchange_rates"),
    partitions_def=exchange_rates_partitions_def,
)


@dg.schedule(
    name="exchange_rates_intraday_schedule",
    job=exchange_rates_daily_job,
    cron_schedule="0 10,13,16 * * *",
    execution_timezone="Europe/Moscow",
)
def exchange_rates_intraday_schedule(context: dg.ScheduleEvaluationContext):
    if context.scheduled_execution_time is not None:
        execution_time_msk = context.scheduled_execution_time.astimezone(MOSCOW_TZ)
    else:
        execution_time_msk = datetime.now(MOSCOW_TZ)

    partition_key = execution_time_msk.date().isoformat()
    slot = execution_time_msk.strftime("%H%M")

    return dg.RunRequest(
        run_key=f"exchange-rates-{partition_key}-{slot}",
        partition_key=partition_key,
    )


def _iter_days(start_day: date, end_day: date):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


@dg.sensor(
    name="exchange_rates_catchup_sensor",
    job=exchange_rates_daily_job,
    minimum_interval_seconds=3600,
)
def exchange_rates_catchup_sensor(context: dg.SensorEvaluationContext):
    # For exchange rates we include today because intraday updates may appear later.
    target_end_day = datetime.now(MOSCOW_TZ).date()

    if target_end_day < EXCHANGE_RATES_START_DATE:
        return dg.SkipReason("No exchange rates partitions yet.")

    missing_run_requests = []

    for current_day in _iter_days(EXCHANGE_RATES_START_DATE, target_end_day):
        partition_key = current_day.isoformat()

        raw_status = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey("raw_exchange_rates_file"),
            partition_keys=[partition_key],
        ).get(partition_key)

        silver_status = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey("silver_exchange_rates_clean"),
            partition_keys=[partition_key],
        ).get(partition_key)

        if (
            raw_status != dg.DagsterAssetPartitionStatus.MATERIALIZED
            or silver_status != dg.DagsterAssetPartitionStatus.MATERIALIZED
        ):
            missing_run_requests.append(
                dg.RunRequest(
                    run_key=f"exchange-rates-catchup-{partition_key}",
                    partition_key=partition_key,
                )
            )

    if not missing_run_requests:
        return dg.SkipReason("No missing exchange rates partitions.")

    return missing_run_requests
