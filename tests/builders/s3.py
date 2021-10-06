from io import BytesIO

from pyarrow import parquet as pq


def read_s3_parquet(bucket, key):
    f = BytesIO()
    bucket.download_fileobj(key, f)
    return pq.read_table(f).to_pydict()


class MockS3Object:
    def __init__(self, bucket, key, contents=None):
        self.bucket = bucket
        self.key = key
        self._contents = contents
        self._metadata = None

    def get(self):
        return {"Body": BytesIO(self._contents)}

    def put(self, **kwargs):
        self._contents = kwargs.get("Body")
        self._metadata = kwargs.get("Metadata")

    def read_parquet(self):
        return pq.read_table(self._contents)

    def get_metadata(self):
        return self._metadata


class MockS3:
    def __init__(self, objects=None):
        self._objects = {}
        if objects is not None:
            self._objects.update({(obj.bucket, obj.key): obj for obj in objects})
        self.Object = self.object

    def object(self, bucket_name, key):
        object_path = (bucket_name, key)
        if object_path not in self._objects:
            self._objects[object_path] = MockS3Object(bucket_name, key)
        return self._objects.get(object_path)
