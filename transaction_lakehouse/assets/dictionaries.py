import os
import subprocess
from pathlib import Path

import dagster as dg

from transaction_lakehouse.resources.minio import MinioResource
from transaction_lakehouse.utils.minio_io import upload_url_to_minio


PROJECT_DIR = Path("/home/ubuntu/lab8_lakehouse")
RAW_BUCKET = os.environ.get("MINIO_RAW_BUCKET", "raw")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
SPARK_JARS = os.environ["SPARK_JARS"]

DICTIONARY_SOURCES = {
    "dictionaries/users.jsonl": "https://storage.yandexcloud.net/npl-de18-lab8-data/reference/users.jsonl",
    "dictionaries/test_users.jsonl": "https://storage.yandexcloud.net/npl-de18-lab8-data/reference/test_users.jsonl",
    "dictionaries/promo_codes.jsonl": "https://storage.yandexcloud.net/npl-de18-lab8-data/reference/promo_codes.jsonl",
}


@dg.asset(group_name="dictionaries")
def raw_dictionaries_files(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    client = minio.get_client()

    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)

    uploaded_files = []

    for object_name, url in DICTIONARY_SOURCES.items():
        context.log.info(f"Downloading {url}")
        context.log.info(f"Uploading to s3://{RAW_BUCKET}/{object_name}")

        result = upload_url_to_minio(
            client=client,
            url=url,
            bucket_name=RAW_BUCKET,
            object_name=object_name,
        )

        uploaded_files.append(result)

    total_size_bytes = sum(item["file_size_bytes"] for item in uploaded_files)

    return dg.MaterializeResult(
        metadata={
            "bucket": RAW_BUCKET,
            "files_count": len(uploaded_files),
            "total_size_bytes": total_size_bytes,
            "objects": [item["object_name"] for item in uploaded_files],
            "source_urls": [item["source_url"] for item in uploaded_files],
        }
    )


@dg.asset(
    group_name="dictionaries",
    deps=[raw_dictionaries_files],
)
def dictionaries_iceberg_tables(
    context: dg.AssetExecutionContext,
    minio: MinioResource,
) -> dg.MaterializeResult:
    client = minio.get_client()

    missing_objects = []

    for object_name in DICTIONARY_SOURCES:
        try:
            client.stat_object(RAW_BUCKET, object_name)
            context.log.info(f"Found s3://{RAW_BUCKET}/{object_name}")
        except Exception as exc:
            context.log.warning(
                f"Missing s3://{RAW_BUCKET}/{object_name}: {exc}"
            )
            missing_objects.append(object_name)

    if missing_objects:
        raise RuntimeError(
            "Missing dictionary files in MinIO: "
            + ", ".join(missing_objects)
        )

    script_path = PROJECT_DIR / "spark_jobs" / "load_dictionaries_to_iceberg.py"

    cmd = [
        SPARK_SUBMIT,
        "--jars",
        SPARK_JARS,
        str(script_path),
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
            "spark_submit": SPARK_SUBMIT,
            "spark_jars": SPARK_JARS,
            "spark_job": str(script_path),
            "source_objects": list(DICTIONARY_SOURCES.keys()),
            "target_tables": [
                "local.dictionaries.users",
                "local.dictionaries.test_users",
                "local.dictionaries.promo_codes",
            ],
            "audit_table": "local.control.file_load_audit",
        }
    )
