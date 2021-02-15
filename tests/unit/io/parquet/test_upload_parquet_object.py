from io import BytesIO
from unittest.mock import MagicMock


from gp2gp.io.parquet import upload_parquet_object
import pyarrow.parquet as pq
import pyarrow as pa


def test_uploads_table():
    mock_s3_object = MagicMock()

    content_table = pa.table([["open"]], ["status"])

    upload_parquet_object(content_table, mock_s3_object)

    expected_table = pa.table([["open"]], ["status"])

    actual_body = mock_s3_object.put.call_args_list[0][1]["Body"]
    actual_table = pq.read_table(BytesIO(actual_body))

    assert actual_table.equals(expected_table)
