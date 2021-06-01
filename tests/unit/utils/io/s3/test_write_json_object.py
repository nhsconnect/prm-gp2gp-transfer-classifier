from datetime import datetime

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


@mock_s3
def test_writes_dictionary_with_timestamp():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3 = S3DataManager(conn)
    data = {"timestamp": datetime(2020, 7, 23)}

    expected = b'{"timestamp": "2020-07-23T00:00:00"}'

    s3.write_json("s3://test_bucket/test_object.json", data)

    actual = bucket.Object("test_object.json").get()["Body"].read()

    assert actual == expected


@mock_s3
def test_writes_correct_content_type():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3 = S3DataManager(conn)
    data = {"fruit": "mango"}

    expected = "application/json"

    s3.write_json("s3://test_bucket/test_object.json", data)

    actual = bucket.Object("test_object.json").get()["ContentType"]

    assert actual == expected
