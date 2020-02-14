from unittest.mock import MagicMock
from gp2gp.odsportal.sources import fetch_practice_data


def test_returns_a_list_of_practices():
    mock_client = MagicMock()
    mock_response = MagicMock()
    binary_content = b'{"Organisations": [{"Name": "GP Practice", "OrgId": "A12345"}]}'
    mock_response.content = binary_content

    mock_client.get.side_effect = [mock_response]

    expected = [{"Name": "GP Practice", "OrgId": "A12345"}]

    actual = fetch_practice_data(mock_client)

    assert actual == expected


def test_returns_combined_list_of_practices_given_several_pages_query():
    next_page = {"Next-Page": "https://test.link"}
    mock_client = MagicMock()
    mock_response_1 = MagicMock()
    mock_response_2 = MagicMock()
    mock_response_3 = MagicMock()

    binary_content_page_1 = b'{"Organisations": [{"Name": "GP Practice", "OrgId": "A12345"}]}'
    binary_content_page_2 = b'{"Organisations": [{"Name": "GP Practice 2", "OrgId": "B64573"}]}'
    binary_content_page_3 = b'{"Organisations": [{"Name": "GP Practice 3", "OrgId": "Y23467"}]}'

    mock_response_1.content = binary_content_page_1
    mock_response_2.content = binary_content_page_2
    mock_response_3.content = binary_content_page_3

    mock_response_1.headers = next_page
    mock_response_2.headers = next_page

    mock_client.get.side_effect = [mock_response_1, mock_response_2, mock_response_3]

    expected = [
        {"Name": "GP Practice", "OrgId": "A12345"},
        {"Name": "GP Practice 2", "OrgId": "B64573"},
        {"Name": "GP Practice 3", "OrgId": "Y23467"},
    ]

    actual = fetch_practice_data(mock_client)

    assert actual == expected
