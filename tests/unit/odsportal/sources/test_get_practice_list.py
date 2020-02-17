from unittest.mock import MagicMock

from gp2gp.odsportal.models import PracticeDetails
from gp2gp.odsportal.sources import get_practice_list
from tests.builders.odsportal import build_mock_response


def test_returns_practice_list_for_single_practice():
    mock_client = MagicMock()
    mock_client.get.return_value = build_mock_response(
        b'{"Organisations": [{"Name": "GP Practice", "OrgId": "A12345"}]}'
    )

    actual = get_practice_list(mock_client)

    expected = [PracticeDetails(ods_code="A12345", name="GP Practice")]

    assert actual == expected
