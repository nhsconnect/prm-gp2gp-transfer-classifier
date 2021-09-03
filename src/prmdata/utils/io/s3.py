import csv
import gzip
import logging
from urllib.parse import urlparse

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
            extra={"event": "READING_FILE_FROM_S3"},
        )
        s3_object = self._object_from_uri(object_uri)
        response = s3_object.get()
        body = response["Body"]
        with gzip.open(body, mode="rt") as f:
            input_csv = csv.DictReader(f)
            yield from input_csv
