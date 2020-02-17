from unittest.mock import MagicMock


def build_mock_response(content, next_page=None):
    mock_response = MagicMock()
    mock_response.content = content
    mock_response.status_code = 200
    if next_page is not None:
        mock_response.headers = {"Next-Page": next_page}
    return mock_response
