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
TRANSACTIONS_START_DATE = date(2026, 4, 27)

SLOTS = [
    f"{hour:02d}-{minute:02d}"
    for hour in range(24)
    for minute in range(0, 60, 10)
]

transactions_partitions_def = dg.MultiPartitionsDefinition(
    {
        "day": dg.DailyPartitionsDefinition(
            start_date=TRANSACTIONS_START_DATE.isoformat(),
            end_offset=1,
        ),
        "slot": dg.StaticPartitionsDefinition(SLOTS),
    }
)

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "day={day}/slot={slot}/transactions.jsonl"
)


def _get_day_slot_from_context(context: dg.AssetExecutionContext) -> tuple[str, str]:
    partition_key = context.partition_key

    if not isinstance(partition_key, dg.MultiPartitionKey):
        raise RuntimeError(
            f"Expected MultiPartitionKey for transactions asset, got: {partition_key!r}"
        )

    return (
        partition_key.keys_by_dimension["day"],
        partition_key.keys_by_dimension["slot"],
    )


def _floor_to_10_min_slot(dt: datetime) -> datetime:
    minute = (dt.minute // 10) * 10
    return dt.replace(minute=minute, second=0, microsecond=0)


def _previous_completed_slot(execution_time_msk: datetime) -> tuple[str, str]:
    target_dt = _floor_to_10_min_slot(execution_time_msk - timedelta(minutes=10))
    return target_dt.date().isoformat(), target_dt.strftime("%H-%M")


def _iter_slots_between(start_dt: datetime, end_dt: datetime):
    current = _floor_to_10_min_slot(start_dt)
    end = _floor_to_10_min_slot(end_dt)

    while current <= end:
        yield current.date().isoformat(), current.strftime("%H-%M")
        current += timedelta(minutes=10)


@dg.asset(
    group_name="transactions",
    partitions_def=transactions_partitions_def,
)
def raw_transactions_file(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    day, slot = _get_day_slot_from_context(context)

    source_url = SOURCE_URL_TEMPLATE.format(day=day, slot=slot)
    object_name = f"transactions/day={day}/slot={slot}/transactions.jsonl"

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

    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Transactions file does not exist yet: {source_url}"
        ) from exc

    return dg.MaterializeResult(
        metadata={
            "partition_day": day,
            "slot": slot,
            "source_url": source_url,
            "bucket": RAW_BUCKET,
            "object_name": object_name,
            "file_size_bytes": result["file_size_bytes"],
        }
    )


@dg.asset(
    group_name="transactions",
    partitions_def=transactions_partitions_def,
    deps=[raw_transactions_file],
)
def silver_transactions_clean(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    day, slot = _get_day_slot_from_context(context)

    object_name = f"transactions/day={day}/slot={slot}/transactions.jsonl"

    client = minio.get_client()

    try:
        client.stat_object(RAW_BUCKET, object_name)
        context.log.info(f"Found s3://{RAW_BUCKET}/{object_name}")
    except Exception as exc:
        raise RuntimeError(
            f"Missing transactions file in MinIO: s3://{RAW_BUCKET}/{object_name}"
        ) from exc

    script_path = PROJECT_DIR / "spark_jobs" / "load_transactions_silver.py"

    cmd = [
        SPARK_SUBMIT,
        "--jars",
        SPARK_JARS,
        str(script_path),
        "--day",
        day,
        "--slot",
        slot,
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
            "slot": slot,
            "spark_submit": SPARK_SUBMIT,
            "spark_jars": SPARK_JARS,
            "spark_job": str(script_path),
            "source_object": object_name,
            "target_table": "local.silver.transactions_clean",
            "audit_table": "local.control.file_load_audit",
        }
    )


transactions_ingest_job = dg.define_asset_job(
    name="transactions_ingest_job",
    selection=dg.AssetSelection.groups("transactions"),
    partitions_def=transactions_partitions_def,
)


@dg.schedule(
    name="transactions_ingest_schedule",
    job=transactions_ingest_job,
    cron_schedule="2,12,22,32,42,52 * * * *",
    execution_timezone="Europe/Moscow",
)
def transactions_ingest_schedule(context: dg.ScheduleEvaluationContext):
    if context.scheduled_execution_time is not None:
        execution_time_msk = context.scheduled_execution_time.astimezone(MOSCOW_TZ)
    else:
        execution_time_msk = datetime.now(MOSCOW_TZ)

    day, slot = _previous_completed_slot(execution_time_msk)

    return dg.RunRequest(
        run_key=f"transactions-{day}-{slot}",
        partition_key=dg.MultiPartitionKey(
            {
                "day": day,
                "slot": slot,
            }
        ),
    )


@dg.sensor(
    name="transactions_catchup_sensor",
    job=transactions_ingest_job,
    minimum_interval_seconds=600,
)
def transactions_catchup_sensor(context: dg.SensorEvaluationContext):
    now_msk = datetime.now(MOSCOW_TZ)
    target_day, target_slot = _previous_completed_slot(now_msk)

    start_dt = datetime.combine(
        TRANSACTIONS_START_DATE,
        datetime.min.time(),
        tzinfo=MOSCOW_TZ,
    )

    end_hour, end_minute = map(int, target_slot.split("-"))
    end_dt = datetime(
        int(target_day[:4]),
        int(target_day[5:7]),
        int(target_day[8:10]),
        end_hour,
        end_minute,
        tzinfo=MOSCOW_TZ,
    )

    max_run_requests_per_tick = 20
    run_requests = []

    for day, slot in _iter_slots_between(start_dt, end_dt):
        partition_key = dg.MultiPartitionKey(
            {
                "day": day,
                "slot": slot,
            }
        )

        raw_status = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey("raw_transactions_file"),
            partition_keys=[partition_key],
        ).get(partition_key)

        silver_status = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey("silver_transactions_clean"),
            partition_keys=[partition_key],
        ).get(partition_key)

        if (
            raw_status != dg.DagsterAssetPartitionStatus.MATERIALIZED
            or silver_status != dg.DagsterAssetPartitionStatus.MATERIALIZED
        ):
            run_requests.append(
                dg.RunRequest(
                    run_key=f"transactions-catchup-{day}-{slot}",
                    partition_key=partition_key,
                )
            )

        if len(run_requests) >= max_run_requests_per_tick:
            break

    if not run_requests:
        return dg.SkipReason("No missing transactions partitions.")

    return run_requests
