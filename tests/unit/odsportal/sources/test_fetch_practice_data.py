from unittest.mock import MagicMock
from gp2gp.odsportal.sources import fetch_practice_data


def test_returns_a_list_of_practices():
    mock_client = MagicMock()
    binary_content = b'{"Organisations": [{"Name": "GP Practice", "OrgId": "A12345"}]}'
    mock_client.get.return_value.content = binary_content

    expected = [{"Name": "GP Practice", "OrgId": "A12345"}]

    actual = fetch_practice_data(mock_client)

    assert actual == expected
