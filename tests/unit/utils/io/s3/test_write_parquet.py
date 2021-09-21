from unittest import mock

import boto3
from moto import mock_s3
import pyarrow as pa

from prmdata.utils.io.s3 import S3DataManager, logger
from tests.builders.file import read_s3_parquet
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION


@mock_s3
def test_writes_table_as_parquet():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")

    data = {"fruit": ["mango", "lemon"]}
    fruit_table = pa.table(data)

    s3_manager = S3DataManager(conn)
    s3_manager.write_parquet(fruit_table, "s3://test_bucket/fruits.parquet")

    actual = read_s3_parquet(bucket, "fruits.parquet")

    assert actual == data


@mock_s3
def test_will_log_writing_table_events():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket_name = "test_bucket"
    conn.create_bucket(Bucket=bucket_name)
    data = {"fruit": ["mango", "lemon"]}
    fruit_table = pa.table(data)

    s3_manager = S3DataManager(conn)

    object_uri = f"s3://{bucket_name}/test_object.parquet"

    with mock.patch.object(logger, "info") as mock_log_info:
        s3_manager.write_parquet(fruit_table, object_uri)
        mock_log_info.assert_has_calls(
            [
                mock.call(
                    f"Attempting to upload: {object_uri}",
                    extra={"event": "ATTEMPTING_UPLOAD_PARQUET_TO_S3", "object_uri": object_uri},
                ),
                mock.call(
                    f"Successfully uploaded to: {object_uri}",
                    extra={"event": "UPLOADED_PARQUET_TO_S3", "object_uri": object_uri},
                ),
            ]
        )
