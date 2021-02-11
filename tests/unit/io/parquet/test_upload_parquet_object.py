from datetime import datetime
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd

from gp2gp.io.parquet import upload_parquet_object


def _convert_to_parquet_format(content):
    df = pd.DataFrame(content)
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer.getvalue()


def test_uploads_dictionary():
    mock_s3_object = MagicMock()

    content = [{"status": "open"}]

    upload_parquet_object(content, mock_s3_object)

    expected_body = _convert_to_parquet_format(content)

    mock_s3_object.put.assert_called_once_with(Body=expected_body)


def test_uploads_dictionary_with_datetime():
    mock_s3_object = MagicMock()

    content = [{"timestamp": datetime(2020, 7, 23)}]

    upload_parquet_object(content, mock_s3_object)

    expected_body = _convert_to_parquet_format(content)

    mock_s3_object.put.assert_called_once_with(Body=expected_body)
