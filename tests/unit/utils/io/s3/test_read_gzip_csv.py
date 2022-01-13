from unittest import mock

import boto3
from moto import mock_s3

from prmdata.utils.input_output.s3 import S3DataManager, logger
from tests.builders.file import build_gzip_csv
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION


@mock_s3
def test_returns_csv_row_as_dictionary():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")
    s3_object = bucket.Object("test_object.csv.gz")
    s3_object.put(
        Body=build_gzip_csv(
            header=["id", "message", "comment"],
            rows=[["123", "A message", "A comment"], ["321", "Another message", "Another comment"]],
        )
    )

    s3_manager = S3DataManager(conn)

    expected = [
        {"id": "123", "message": "A message", "comment": "A comment"},
        {"id": "321", "message": "Another message", "comment": "Another comment"},
    ]

    actual = s3_manager.read_gzip_csv("s3://test_bucket/test_object.csv.gz")

    assert list(actual) == expected


@mock_s3
def test_will_log_reading_file_event():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket_name = "test_bucket"
    bucket = conn.create_bucket(Bucket=bucket_name)
    s3_object = bucket.Object("test_object.csv.gz")
    s3_object.put(
        Body=build_gzip_csv(
            header=["id", "message", "comment"],
            rows=[["123", "A message", "A comment"], ["321", "Another message", "Another comment"]],
        )
    )

    s3_manager = S3DataManager(conn)
    object_uri = f"s3://{bucket_name}/test_object.csv.gz"

    with mock.patch.object(logger, "info") as mock_log_info:
        gzip_csv = s3_manager.read_gzip_csv(object_uri)
        list(gzip_csv)
        mock_log_info.assert_called_once_with(
            f"Reading file from: {object_uri}",
            extra={"event": "READING_FILE_FROM_S3", "object_uri": object_uri},
        )
