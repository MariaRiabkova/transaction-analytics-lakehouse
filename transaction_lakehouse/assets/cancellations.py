import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

import dagster as dg

from transaction_lakehouse.resources.minio import MinioResource
from transaction_lakehouse.utils.minio_io import upload_url_to_minio


PROJECT_DIR = Path("/home/ubuntu/lab8_lakehouse")
RAW_BUCKET = os.environ.get("MINIO_RAW_BUCKET", "raw")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
SPARK_JARS = os.environ["SPARK_JARS"]

CANCELLATIONS_START_DATE = date(2026, 4, 27)

cancellations_partitions_def = dg.DailyPartitionsDefinition(
    start_date=CANCELLATIONS_START_DATE.isoformat(),
)

SOURCE_URL_TEMPLATE = (
    "https://storage.yandexcloud.net/npl-de18-lab8-data/"
    "cancellations/day={day}/cancellations.jsonl"
)


@dg.asset(
    group_name="cancellations",
    partitions_def=cancellations_partitions_def,
)
def raw_cancellations_file(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    day = context.partition_key
    source_url = SOURCE_URL_TEMPLATE.format(day=day)
    object_name = f"cancellations/day={day}/cancellations.jsonl"

    client = minio.get_client()

    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)

    context.log.info(f"Downloading {source_url}")
    context.log.info(f"Uploading to s3://{RAW_BUCKET}/{object_name}")

    result = upload_url_to_minio(
        client=client,
        url=source_url,
        bucket_name=RAW_BUCKET,
        object_name=object_name,
    )

    return dg.MaterializeResult(
        metadata={
            "partition_day": day,
            "source_url": source_url,
            "bucket": RAW_BUCKET,
            "object_name": object_name,
            "file_size_bytes": result["file_size_bytes"],
        }
    )


@dg.asset(
    group_name="cancellations",
    partitions_def=cancellations_partitions_def,
    deps=[raw_cancellations_file],
)
def silver_cancellations_clean(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    day = context.partition_key
    object_name = f"cancellations/day={day}/cancellations.jsonl"

    client = minio.get_client()

    try:
        client.stat_object(RAW_BUCKET, object_name)
        context.log.info(f"Found s3://{RAW_BUCKET}/{object_name}")
    except Exception as exc:
        raise RuntimeError(
            f"Missing cancellations file in MinIO: s3://{RAW_BUCKET}/{object_name}"
        ) from exc

    script_path = PROJECT_DIR / "spark_jobs" / "load_cancellations_silver.py"

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
            "spark_submit": SPARK_SUBMIT,
            "spark_jars": SPARK_JARS,
            "spark_job": str(script_path),
            "source_object": object_name,
            "target_table": "local.silver.cancellations_clean",
            "audit_table": "local.control.file_load_audit",
        }
    )


cancellations_daily_job = dg.define_asset_job(
    name="cancellations_daily_job",
    selection=dg.AssetSelection.groups("cancellations"),
    partitions_def=cancellations_partitions_def,
)


def _iter_days(start_day: date, end_day: date):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


@dg.sensor(
    name="cancellations_catchup_sensor",
    job=cancellations_daily_job,
    minimum_interval_seconds=3600,
)
def cancellations_catchup_sensor(context: dg.SensorEvaluationContext):
    # Load only completed days. Today's file may still be incomplete.
    target_end_day = datetime.utcnow().date() - timedelta(days=1)

    if target_end_day < CANCELLATIONS_START_DATE:
        return dg.SkipReason("No completed cancellations partitions yet.")

    missing_run_requests = []

    for current_day in _iter_days(CANCELLATIONS_START_DATE, target_end_day):
        partition_key = current_day.isoformat()

        raw_status = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey("raw_cancellations_file"),
            partition_keys=[partition_key],
        ).get(partition_key)

        silver_status = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey("silver_cancellations_clean"),
            partition_keys=[partition_key],
        ).get(partition_key)

        if (
            raw_status != dg.DagsterAssetPartitionStatus.MATERIALIZED
            or silver_status != dg.DagsterAssetPartitionStatus.MATERIALIZED
        ):
            missing_run_requests.append(
                dg.RunRequest(
                    run_key=f"cancellations-{partition_key}",
                    partition_key=partition_key,
                )
            )

    if not missing_run_requests:
        return dg.SkipReason("No missing completed cancellations partitions.")

    return missing_run_requests
