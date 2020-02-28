from datetime import datetime
from dateutil.tz import tzutc
from freezegun import freeze_time

from gp2gp.odsportal.models import PracticeDetails
from gp2gp.odsportal.sources import construct_practice_list_from_ods_portal_response
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


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42), tz_offset=0)
def test_has_correct_generated_on_timestamp_given_time():
    empty_response = []

    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42, tzinfo=tzutc())
    actual = construct_practice_list_from_ods_portal_response(empty_response)

    assert actual.generated_on == expected_generated_on


def test_returns_single_practice():
    response_data = [_build_practice_data(name="GP Practice", org_id="A12345")]

    expected_practices = [PracticeDetails(ods_code="A12345", name="GP Practice")]
    actual = construct_practice_list_from_ods_portal_response(response_data)

    assert actual.practices == expected_practices


def test_returns_multiple_practices():
    response_data = [
        _build_practice_data(name="GP Practice", org_id="A12345"),
        _build_practice_data(name="GP Practice 2", org_id="B56789"),
        _build_practice_data(name="GP Practice 3", org_id="C56789"),
    ]

    expected_practices = [
        PracticeDetails(ods_code="A12345", name="GP Practice"),
        PracticeDetails(ods_code="B56789", name="GP Practice 2"),
        PracticeDetails(ods_code="C56789", name="GP Practice 3"),
    ]
    actual = construct_practice_list_from_ods_portal_response(response_data)

    assert actual.practices == expected_practices
