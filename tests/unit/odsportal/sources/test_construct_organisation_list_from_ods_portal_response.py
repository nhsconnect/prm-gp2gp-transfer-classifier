from datetime import datetime
from dateutil.tz import tzutc
from freezegun import freeze_time

from gp2gp.odsportal.models import OrganisationDetails
from gp2gp.odsportal.sources import construct_organisation_metadata_from_ods_portal_response
from tests.builders.common import a_string


def _build_organisation_data(**kwargs):
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
    actual = construct_organisation_metadata_from_ods_portal_response(
        empty_response, empty_response
    )

    assert actual.generated_on == expected_generated_on


def test_returns_single_practice_and_single_ccg():
    response_practice_data = [_build_organisation_data(name="GP Practice", org_id="A12345")]
    response_ccg_data = [_build_organisation_data(name="CCG", org_id="12C")]

    expected_practices = [OrganisationDetails(ods_code="A12345", name="GP Practice")]
    expected_ccgs = [OrganisationDetails(ods_code="12C", name="CCG")]
    actual = construct_organisation_metadata_from_ods_portal_response(
        response_practice_data, response_ccg_data
    )

    assert actual.practices == expected_practices
    assert actual.ccgs == expected_ccgs


def test_returns_multiple_practices_and_ccgs():
    response_practice_data = [
        _build_organisation_data(name="GP Practice", org_id="A12345"),
        _build_organisation_data(name="GP Practice 2", org_id="B56789"),
        _build_organisation_data(name="GP Practice 3", org_id="C56789"),
    ]

    response_ccg_data = [
        _build_organisation_data(name="CCG", org_id="12A"),
        _build_organisation_data(name="CCG 2", org_id="34A"),
        _build_organisation_data(name="CCG 3", org_id="56A"),
    ]

    expected_practices = [
        OrganisationDetails(ods_code="A12345", name="GP Practice"),
        OrganisationDetails(ods_code="B56789", name="GP Practice 2"),
        OrganisationDetails(ods_code="C56789", name="GP Practice 3"),
    ]

    expected_ccgs = [
        OrganisationDetails(ods_code="12A", name="CCG"),
        OrganisationDetails(ods_code="34A", name="CCG 2"),
        OrganisationDetails(ods_code="56A", name="CCG 3"),
    ]

    actual = construct_organisation_metadata_from_ods_portal_response(
        response_practice_data, response_ccg_data
    )

    assert actual.practices == expected_practices
    assert actual.ccgs == expected_ccgs


def test_returns_unique_practices_and_ccgs_in_practice_list():
    response_practice_data = [
        _build_organisation_data(name="GP Practice", org_id="A12345"),
        _build_organisation_data(name="Another GP Practice", org_id="A12345"),
    ]

    response_ccg_data = [
        _build_organisation_data(name="CCG", org_id="12A"),
        _build_organisation_data(name="Another CCG", org_id="12A"),
    ]

    actual = construct_organisation_metadata_from_ods_portal_response(
        response_practice_data, response_ccg_data
    )

    assert len(actual.practices) == 1
    assert len(actual.ccgs) == 1