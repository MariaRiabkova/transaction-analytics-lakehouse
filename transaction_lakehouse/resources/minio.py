import os

import dagster as dg
from minio import Minio


class MinioResource(dg.ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = False

    def get_client(self) -> Minio:
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )


def make_minio_resource() -> MinioResource:
    return MinioResource(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
