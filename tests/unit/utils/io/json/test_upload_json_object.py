from datetime import datetime
from unittest.mock import MagicMock

from prmdata.utils.io.json import upload_json_object


def test_uploads_dictionary():
    mock_s3_object = MagicMock()

    content = {"status": "open"}

    upload_json_object(content, mock_s3_object)

    expected_body = b'{"status": "open"}'

    mock_s3_object.put.assert_called_once_with(Body=expected_body, ContentType="application/json")


def test_uploads_dictionary_with_datetime():
    mock_s3_object = MagicMock()

    content = {"timestamp": datetime(2020, 7, 23)}

    upload_json_object(content, mock_s3_object)

    expected_body = b'{"timestamp": "2020-07-23T00:00:00"}'

    mock_s3_object.put.assert_called_once_with(Body=expected_body, ContentType="application/json")
