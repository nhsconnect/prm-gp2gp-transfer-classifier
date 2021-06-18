import csv
import gzip
import json
from datetime import datetime
from urllib.parse import urlparse

from mypy_boto3_s3 import S3ServiceResource


def _serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


class S3DataManager:
    def __init__(self, client: S3ServiceResource):
        self._client = client

    def _object_from_uri(self, uri):
        object_url = urlparse(uri)
        s3_bucket = object_url.netloc
        s3_key = object_url.path.lstrip("/")
        return self._client.Object(s3_bucket, s3_key)

    def read_json(self, object_uri):
        s3_object = self._object_from_uri(object_uri)
        response = s3_object.get()
        body = response["Body"].read()
        return json.loads(body.decode("utf8"))

    def write_json(self, object_uri, data):
        s3_object = self._object_from_uri(object_uri)
        body = json.dumps(data, default=_serialize_datetime).encode("utf8")
        s3_object.put(Body=body, ContentType="application/json")

    def read_gzip_csv(self, object_uri):
        s3_object = self._object_from_uri(object_uri)
        response = s3_object.get()
        body = response["Body"]
        with gzip.open(body, mode="rt") as f:
            input_csv = csv.DictReader(f)
            yield from input_csv
