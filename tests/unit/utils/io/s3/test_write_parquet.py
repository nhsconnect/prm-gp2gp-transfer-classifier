from unittest import mock

import boto3
import pyarrow as pa
from moto import mock_s3

from prmdata.utils.input_output.s3 import S3DataManager, logger
from tests.builders.s3 import read_s3_parquet
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION

SOME_METADATA = {"metadata_field": "metadata_value"}


@mock_s3
def test_writes_table_as_parquet():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket = conn.create_bucket(Bucket="test_bucket")

    data = {"fruit": ["mango", "lemon"]}
    fruit_table = pa.table(data)

    s3_manager = S3DataManager(conn)
    s3_manager.write_parquet(fruit_table, "s3://test_bucket/fruits.parquet", SOME_METADATA)

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
        s3_manager.write_parquet(fruit_table, object_uri, SOME_METADATA)
        mock_log_info.assert_has_calls(
            [
                mock.call(
                    f"Attempting to upload: {object_uri}",
                    extra={
                        "event": "ATTEMPTING_UPLOAD_PARQUET_TO_S3",
                        "object_uri": object_uri,
                        "metadata": SOME_METADATA,
                    },
                ),
                mock.call(
                    f"Successfully uploaded to: {object_uri}",
                    extra={
                        "event": "SUCCESSFULLY_UPLOADED_PARQUET_TO_S3",
                        "object_uri": object_uri,
                        "metadata": SOME_METADATA,
                    },
                ),
                mock.call(
                    f"Transfer classifier row count for: {object_uri}",
                    extra={
                        "event": "TRANSFER_CLASSIFIER_ROW_COUNT",
                        "object_uri": object_uri,
                        "row_count": 2,
                        "metadata": SOME_METADATA,
                    },
                ),
            ]
        )


@mock_s3
def test_will_write_metatdata():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    bucket_name = "test_bucket"
    bucket = conn.create_bucket(Bucket=bucket_name)

    data = {"fruit": ["mango", "lemon"]}
    fruit_table = pa.table(data)

    metadata = {
        "metadata_field": "metadata_field_value",
        "second_metadata_field": "metadata_field_second_value",
    }

    s3_manager = S3DataManager(conn)

    s3_manager.write_parquet(
        table=fruit_table, object_uri=f"s3://{bucket_name}/test_object.parquet", metadata=metadata
    )

    expected = metadata
    actual = bucket.Object("test_object.parquet").get()["Metadata"]

    assert actual == expected
