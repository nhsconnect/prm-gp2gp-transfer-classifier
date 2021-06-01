import json
from urllib.parse import urlparse


class S3DataManager:
    def __init__(self, client):
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
        body = json.dumps(data)
        s3_object.put(Body=body)
