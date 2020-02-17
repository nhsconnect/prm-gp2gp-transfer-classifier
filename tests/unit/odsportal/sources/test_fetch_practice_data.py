from unittest.mock import MagicMock
import pytest

from gp2gp.odsportal.sources import fetch_practice_data, OdsPortalException


def _mock_response(content, next_page=None):
    mock_response = MagicMock()
    mock_response.content = content
    mock_response.status_code = 200
    if next_page is not None:
        mock_response.headers = {"Next-Page": next_page}
    return mock_response


def test_returns_a_list_of_practices():
    mock_client = MagicMock()
    mock_response = _mock_response(
        b'{"Organisations": [{"Name": "GP Practice", "OrgId": "A12345"}]}'
    )

    mock_client.get.side_effect = [mock_response]

    expected = [{"Name": "GP Practice", "OrgId": "A12345"}]

    actual = fetch_practice_data(mock_client)

    assert actual == expected


def test_returns_combined_list_of_practices_given_several_pages_query():
    mock_client = MagicMock()

    url_1 = "https://test.link/1"
    url_2 = "https://test.link/2"
    url_3 = "https://test.link/3"

    pages = {
        url_1: _mock_response(
            b'{"Organisations": [{"Name": "GP Practice", "OrgId": "A12345"}]}', url_2
        ),
        url_2: _mock_response(
            b'{"Organisations": [{"Name": "GP Practice 2", "OrgId": "B64573"}]}', url_3
        ),
        url_3: _mock_response(b'{"Organisations": [{"Name": "GP Practice 3", "OrgId": "Y23467"}]}'),
    }

    mock_client.get.side_effect = lambda *args: pages[args[0]]

    expected = [
        {"Name": "GP Practice", "OrgId": "A12345"},
        {"Name": "GP Practice 2", "OrgId": "B64573"},
        {"Name": "GP Practice 3", "OrgId": "Y23467"},
    ]

    actual = fetch_practice_data(mock_client, url_1)

    assert actual == expected


def test_throws_custom_exception_when_status_code_is_not_200():
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 500

    mock_client.get.side_effect = [mock_response]

    with pytest.raises(OdsPortalException):
        fetch_practice_data(mock_client)
