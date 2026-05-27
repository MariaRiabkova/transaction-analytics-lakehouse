from pathlib import Path
from tempfile import NamedTemporaryFile

import requests
from minio import Minio


def upload_url_to_minio(
    *,
    client: Minio,
    url: str,
    bucket_name: str,
    object_name: str,
    timeout: int = 60,
) -> dict:
    response = requests.get(url, timeout=timeout)

    if response.status_code == 404:
        raise FileNotFoundError(f"File not found: {url}")

    response.raise_for_status()

    with NamedTemporaryFile(delete=False, suffix=".jsonl") as tmp:
        tmp.write(response.content)
        tmp_path = Path(tmp.name)

    file_size_bytes = tmp_path.stat().st_size

    client.fput_object(
        bucket_name,
        object_name,
        str(tmp_path),
        content_type="application/jsonl",
    )

    tmp_path.unlink(missing_ok=True)

    return {
        "source_url": url,
        "bucket": bucket_name,
        "object_name": object_name,
        "file_size_bytes": file_size_bytes,
    }
