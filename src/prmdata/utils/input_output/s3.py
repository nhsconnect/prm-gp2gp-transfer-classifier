import csv
import gzip
import json
import logging
from io import BytesIO
from typing import List
from urllib.parse import urlparse

from pyarrow import PythonFile, Table, parquet

logger = logging.getLogger(__name__)


class JsonFileNotFoundException(Exception):
    def __init__(self, object_uri: str):
        self._object_uri = object_uri
        super().__init__("Unable to locate JSON file in S3 uri: " + object_uri)

    @property
    def missing_json_uri(self) -> str:
        return self._object_uri


class S3DataManager:
    def __init__(self, client):
        self._client = client

    def _object_from_uri(self, uri: str):
        object_url = urlparse(uri)
        s3_bucket = object_url.netloc
        s3_key = object_url.path.lstrip("/")
        return self._client.Object(s3_bucket, s3_key)

    def read_gzip_csv(self, object_uri: str):
        logger.info(
            "Reading file from: " + object_uri,
            extra={"event": "READING_FILE_FROM_S3", "object_uri": object_uri},
        )
        s3_object = self._object_from_uri(object_uri)

        try:
            response = s3_object.get()
            body = response["Body"]
            with gzip.open(body, mode="rt") as f:
                input_csv = csv.DictReader(f)
                yield from input_csv

        except self._client.meta.client.exceptions.NoSuchKey as e:
            logger.error(
                f"CSV file not found: {object_uri}, exiting...",
                extra={"event": "FILE_NOT_FOUND_IN_S3", "object_uri": object_uri},
            )
            raise e

    def read_json(self, object_uri: str):
        logger.info(
            "Reading file from: " + object_uri,
            extra={"event": "READING_FILE_FROM_S3", "object_uri": object_uri},
        )
        s3_object = self._object_from_uri(object_uri)

        try:
            response = s3_object.get()
        except self._client.meta.client.exceptions.NoSuchKey:
            logger.error(
                f"JSON file not found: {object_uri}, exiting...",
                extra={"event": "FILE_NOT_FOUND_IN_S3", "object_uri": object_uri},
            )
            raise JsonFileNotFoundException(object_uri)

        body = response["Body"].read()
        return json.loads(body.decode("utf8"))

    def write_parquet(self, table: Table, object_uri: str, metadata: dict[str, str]):
        logger.info(
            f"Attempting to upload: {object_uri}",
            extra={
                "event": "ATTEMPTING_UPLOAD_PARQUET_TO_S3",
                "object_uri": object_uri,
                "metadata": metadata,
            },
        )

        s3_object = self._object_from_uri(object_uri)
        buffer = BytesIO()
        buffer_file = PythonFile(buffer)
        parquet.write_table(table, buffer_file)
        buffer.seek(0)

        s3_object.put(Body=buffer, Metadata=metadata)

        logger.info(
            f"Successfully uploaded to: {object_uri}",
            extra={
                "event": "SUCCESSFULLY_UPLOADED_PARQUET_TO_S3",
                "object_uri": object_uri,
                "metadata": metadata,
            },
        )

        logger.info(
            f"Transfer classifier row count for: {object_uri}",
            extra={
                "event": "TRANSFER_CLASSIFIER_ROW_COUNT",
                "object_uri": object_uri,
                "row_count": table.num_rows,
                "metadata": metadata,
            },
        )

    def _read_json_files(self, s3_path: str) -> List[dict]:
        object_url = urlparse(s3_path)
        bucket_name = object_url.netloc
        s3_key = object_url.path.lstrip("/")

        s3_bucket = self._client.Bucket(bucket_name)
        s3_files = s3_bucket.objects.filter(Prefix=s3_key).all()

        return [json.load(s3_file.get()["Body"]) for s3_file in s3_files]

    def read_json_files_from_paths(self, s3_paths: List[str]) -> List[dict]:
        mi_events = []

        for path in s3_paths:
            mi_events.extend(self._read_json_files(path))
        return mi_events
