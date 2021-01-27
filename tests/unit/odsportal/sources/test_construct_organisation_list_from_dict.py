from datetime import datetime

from gp2gp.odsportal.models import OrganisationDetails, OrganisationDetailsWithAsid
from gp2gp.odsportal.sources import construct_organisation_list_from_dict


def test_returns_model_with_generated_on_timestamp():
    data = {"generated_on": "2020-07-23T00:00:00", "practices": [], "ccgs": []}

    expected_timestamp = datetime(2020, 7, 23)
    actual = construct_organisation_list_from_dict(data)

    assert actual.generated_on == expected_timestamp


def test_returns_list_with_one_practice_and_one_ccg():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "practices": [{"ods_code": "A12345", "name": "GP Practice", "asid": "123456789123"}],
        "ccgs": [{"ods_code": "12A", "name": "CCG"}],
    }

    expected_practices = [
        OrganisationDetailsWithAsid(asid="123456789123", ods_code="A12345", name="GP Practice")
    ]
    expected_ccgs = [OrganisationDetails(ods_code="12A", name="CCG")]
    actual = construct_organisation_list_from_dict(data)

    assert actual.practices == expected_practices
    assert actual.ccgs == expected_ccgs


def test_returns_list_with_multiple_practices_and_ccgs():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "practices": [
            {"ods_code": "A12345", "name": "GP Practice", "asid": "223456789123"},
            {"ods_code": "B12345", "name": "GP Practice 2", "asid": "323456789123"},
            {"ods_code": "C12345", "name": "GP Practice 3", "asid": "423456789123"},
        ],
        "ccgs": [
            {"ods_code": "12A", "name": "CCG"},
            {"ods_code": "34A", "name": "CCG 2"},
            {"ods_code": "56A", "name": "CCG 3"},
        ],
    }

    expected_practices = [
        OrganisationDetailsWithAsid(asid="223456789123", ods_code="A12345", name="GP Practice"),
        OrganisationDetailsWithAsid(asid="323456789123", ods_code="B12345", name="GP Practice 2"),
        OrganisationDetailsWithAsid(asid="423456789123", ods_code="C12345", name="GP Practice 3"),
    ]
    expected_ccgs = [
        OrganisationDetails(ods_code="12A", name="CCG"),
        OrganisationDetails(ods_code="34A", name="CCG 2"),
        OrganisationDetails(ods_code="56A", name="CCG 3"),
    ]
    actual = construct_organisation_list_from_dict(data)

    assert actual.practices == expected_practices
    assert actual.ccgs == expected_ccgs
