from unittest.mock import MagicMock

from gp2gp.io.json import upload_json_object


def test_uploads_dictionary():
    mock_s3_object = MagicMock()

    content = {"status": "open"}

    upload_json_object(mock_s3_object, content)

    mock_s3_object.put.assert_called_once_with(
        Body=b'{"status": "open"}', ContentType="application/json"
    )
