import boto3
from moto import mock_s3

from prmdata.utils.io.s3 import S3DataManager
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION


@mock_s3
def test_read_json_object_returns_dictionary():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3_object = bucket.Object("test_object.json")
    s3_object.put(Body=b'{"fruit": "mango"}')

    s3_manager = S3DataManager(conn)

    expected = {"fruit": "mango"}

    actual = s3_manager.read_json("s3://test_bucket/test_object.json")

    assert actual == expected
