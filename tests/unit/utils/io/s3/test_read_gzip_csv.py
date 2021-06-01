import boto3
from moto import mock_s3

from prmdata.utils.io.s3 import S3DataManager
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
