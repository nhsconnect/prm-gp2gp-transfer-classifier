import boto3
from moto import mock_s3

from prmdata.utils.io.s3 import S3DataManager
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION


@mock_s3
def test_writes_dictionary():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3 = S3DataManager(conn)
    data = {"fruit": "mango"}

    expected = b'{"fruit": "mango"}'

    s3.write_json("s3://test_bucket/test_object.json", data)

    actual = bucket.Object("test_object.json").get()["Body"].read()

    assert actual == expected
