import csv
import gzip
import logging
from io import BytesIO
from urllib.parse import urlparse

from pyarrow import Table, PythonFile, parquet

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
        response = s3_object.get()
        body = response["Body"]
        with gzip.open(body, mode="rt") as f:
            input_csv = csv.DictReader(f)
            yield from input_csv

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
            extra={"event": "UPLOADED_PARQUET_TO_S3", "object_uri": object_uri},
        )
