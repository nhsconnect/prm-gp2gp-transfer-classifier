import boto3
from moto import mock_s3
import pyarrow as pa

from prmdata.utils.io.s3 import S3DataManager
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
