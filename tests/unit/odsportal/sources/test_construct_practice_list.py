from gp2gp.odsportal.models import PracticeDetails
from gp2gp.odsportal.sources import construct_practice_list
from tests.builders.common import a_string


def _build_practice_data(**kwargs):
    return {
        "Name": kwargs.get("name", a_string()),
        "OrgId": kwargs.get("org_id", a_string()),
        "Status": "Active",
        "OrgRecordClass": kwargs.get("org_record_class", a_string()),
        "PostCode": kwargs.get("post_code", a_string()),
        "LastChangeDate": kwargs.get("last_change_date", a_string()),
        "PrimaryRoleId": kwargs.get("primary_role_id", a_string()),
        "PrimaryRoleDescription": kwargs.get("primary_role_description", a_string()),
        "OrgLink": kwargs.get("org_link", a_string()),
    }


def test_returns_single_practice():
    response_data = [_build_practice_data(name="GP Practice", org_id="A12345")]

    actual = construct_practice_list(response_data)

    expected = [PracticeDetails(ods_code="A12345", name="GP Practice")]

    assert actual == expected


def test_returns_two_practices():
    response_data = [
        _build_practice_data(name="GP Practice", org_id="A12345"),
        _build_practice_data(name="Another GP Practice", org_id="B56789"),
    ]

    actual = construct_practice_list(response_data)

    expected = [
        PracticeDetails(ods_code="A12345", name="GP Practice"),
        PracticeDetails(ods_code="B56789", name="Another GP Practice"),
    ]

    assert actual == expected
