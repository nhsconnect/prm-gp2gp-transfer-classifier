from unittest import mock

import boto3
import pytest
from moto import mock_s3

from prmdata.utils.input_output.s3 import JsonFileNotFoundException, S3DataManager, logger
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


@mock_s3
def test_will_log_reading_file_event():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3_object = bucket.Object("test_object.json")
    s3_object.put(Body=b'{"fruit": "mango"}')

    s3_manager = S3DataManager(conn)
    object_uri = "s3://test_bucket/test_object.json"

    with mock.patch.object(logger, "info") as mock_log_info:
        s3_manager.read_json(object_uri)
        mock_log_info.assert_called_once_with(
            f"Reading file from: {object_uri}",
            extra={"event": "READING_FILE_FROM_S3", "object_uri": object_uri},
        )


@mock_s3
def test_throws_json_file_not_found_exception_when_unable_to_read_file_from_s3():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    bucket.Object("test_object.json")

    s3_manager = S3DataManager(conn)
    object_uri = "s3://test_bucket/test_object.json"

    with pytest.raises(JsonFileNotFoundException) as e:
        s3_manager.read_json(object_uri)
    assert str(e.value) == "Unable to locate JSON file in S3 uri: " + object_uri


def test_json_file_not_found_exception_missing_json_uri_property_returns_uri():
    object_uri = "s3://test_bucket/test_object.json"
    actual_exception = JsonFileNotFoundException(object_uri)
    assert actual_exception.missing_json_uri == object_uri
