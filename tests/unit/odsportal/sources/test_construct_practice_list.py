from gp2gp.odsportal.models import PracticeDetails
from gp2gp.odsportal.sources import construct_practice_list
from tests.builders.common import a_string


def test_returns_single_practice():
    response_data = [
        {
            "Name": "GP Practice",
            "OrgId": "A12345",
            "Status": "Active",
            "OrgRecordClass": a_string(),
            "PostCode": a_string(),
            "LastChangeDate": a_string(),
            "PrimaryRoleId": a_string(),
            "PrimaryRoleDescription": a_string(),
            "OrgLink": a_string(),
        }
    ]

    actual = construct_practice_list(response_data)

    expected = [PracticeDetails(ods_code="A12345", name="GP Practice")]

    assert actual == expected
