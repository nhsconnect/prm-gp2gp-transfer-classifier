import json
from urllib.parse import urlparse


class S3Storage:
    def __init__(self, client):
        self._client = client

    def read_json(self, object_url):
        object_url = urlparse(object_url)
        s3_bucket = object_url.netloc
        s3_key = object_url.path.lstrip("/")
        s3_object = self._client.Object(s3_bucket, s3_key)
        response = s3_object.get()
        body = response["Body"].read()
        return json.loads(body.decode("utf8"))
