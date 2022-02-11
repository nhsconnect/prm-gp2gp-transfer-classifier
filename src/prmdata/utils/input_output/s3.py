import csv
import gzip
import json
import logging
import sys
from io import BytesIO
from urllib.parse import urlparse

from pyarrow import PythonFile, Table, parquet

logger = logging.getLogger(__name__)


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

        except self._client.meta.client.exceptions.NoSuchKey:
            logger.error(
                f"File not found: {object_uri}, exiting...",
                extra={"event": "FILE_NOT_FOUND_IN_S3", "object_uri": object_uri},
            )
            sys.exit(1)

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
                f"File not found: {object_uri}, exiting...",
                extra={"event": "FILE_NOT_FOUND_IN_S3", "object_uri": object_uri},
            )
            sys.exit(1)

        body = response["Body"].read()
        return json.loads(body.decode("utf8"))

    def write_parquet(self, table: Table, object_uri: str, metadata: dict[str, str]):
        logger.info(
            f"Attempting to upload: {object_uri}",
            extra={"event": "ATTEMPTING_UPLOAD_PARQUET_TO_S3", "object_uri": object_uri},
        )

        s3_object = self._object_from_uri(object_uri)
        buffer = BytesIO()
        buffer_file = PythonFile(buffer)
        parquet.write_table(table, buffer_file)
        buffer.seek(0)

        s3_object.put(Body=buffer, Metadata=metadata)

        logger.info(
            f"Successfully uploaded to: {object_uri}",
            extra={"event": "SUCCESSFULLY_UPLOADED_PARQUET_TO_S3", "object_uri": object_uri},
        )
