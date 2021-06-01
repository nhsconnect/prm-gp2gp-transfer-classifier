import boto3
from moto import mock_s3

from prmdata.utils.io.s3 import S3Storage

MOTO_MOCK_REGION = "us-east-1"


@mock_s3
def test_read_json_object_returns_dictionary():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3_object = bucket.Object("test_object.json")
    s3_object.put(Body=b'{"fruit": "mango"}')

    s3 = S3Storage(conn)

    expected = {"fruit": "mango"}

    actual = s3.read_json("s3://test_bucket/test_object.json")

    assert actual == expected
