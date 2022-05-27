import boto3
from moto import mock_s3

from prmdata.utils.input_output.s3 import S3DataManager
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION


@mock_s3
def test_get_files_in_generic_path():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket_name = "test_bucket"
    bucket = conn.create_bucket(Bucket=bucket_name)
    s3_path = f"s3://{bucket_name}/v1"
    bucket.Object("v1/event1.json").put(Body=b'{"eventId": "1234", "eventType": "SOME_EVENT"}')
    bucket.Object("v1/2/event2.json").put(
        Body=b'{"eventId": "5678", "eventType": "SOME_OTHER_EVENT"}'
    )

    s3_manager = S3DataManager(conn)

    expected = [
        {"eventId": "5678", "eventType": "SOME_OTHER_EVENT"},
        {"eventId": "1234", "eventType": "SOME_EVENT"},
    ]

    actual = s3_manager.get_json_files_from_path(s3_path=s3_path)

    assert actual == expected


@mock_s3
def test_get_filtered_files_in_specific_path():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket_name = "test_bucket"
    s3_path = f"s3://{bucket_name}/v1/2"
    bucket = conn.create_bucket(Bucket=bucket_name)
    bucket.Object("v1/event1.json").put(Body=b"")
    bucket.Object("v1/2/event2.json").put(Body=b'{"eventId": "1234", "eventType": "SOME_EVENT"}')

    s3_manager = S3DataManager(conn)

    expected = [{"eventId": "1234", "eventType": "SOME_EVENT"}]

    actual = s3_manager.get_json_files_from_path(s3_path=s3_path)

    assert actual == expected
