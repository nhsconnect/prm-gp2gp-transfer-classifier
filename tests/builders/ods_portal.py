from random import randrange
from unittest.mock import MagicMock

from prmdata.domain.ods_portal.models import PracticeDetails
from tests.builders.common import a_string


def an_asid_list():
    return [a_string() for _ in range(randrange(3))]


def build_practice_details(**kwargs):
    return PracticeDetails(
        ods_code=kwargs.get("ods_code", a_string()),
        name=kwargs.get("name", a_string()),
        asids=kwargs.get("asids", an_asid_list()),
    )


def build_mock_response(content=None, status_code=200, next_page=None):
    mock_response = MagicMock()
    mock_response.content = content
    mock_response.status_code = status_code
    if next_page is not None:
        mock_response.headers = {"Next-Page": next_page}
    return mock_response
