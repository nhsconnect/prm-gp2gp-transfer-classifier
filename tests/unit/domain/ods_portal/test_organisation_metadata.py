from datetime import datetime

from prmdata.domain.ods_portal.organisation_metadata import (
    OrganisationMetadata,
    PracticeMetadata,
    SicblMetadata,
)


def test_from_dict_returns_model_with_generated_on_timestamp():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "year": 2020,
        "month": 7,
        "practices": [],
        "sicbls": [],
    }

    expected_timestamp = datetime(2020, 7, 23)
    actual = OrganisationMetadata.from_dict(data)

    assert actual.generated_on == expected_timestamp


def test_from_dict_returns_model_with_year_and_month():
    year = 2020
    month = 7
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "year": year,
        "month": month,
        "practices": [],
        "sicbls": [],
    }

    actual = OrganisationMetadata.from_dict(data)

    assert actual.year == year
    assert actual.month == month


def test_from_dict_returns_list_with_one_practice_and_one_sicbl():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "year": 2020,
        "month": 7,
        "practices": [{"ods_code": "A12345", "name": "GP Practice", "asids": ["123456789123"]}],
        "sicbls": [{"ods_code": "12A", "name": "SICBL", "practices": ["A12345"]}],
    }

    expected_practices = [
        PracticeMetadata(asids=["123456789123"], ods_code="A12345", name="GP Practice")
    ]
    expected_sicbls = [SicblMetadata(ods_code="12A", name="SICBL", practices=["A12345"])]
    actual = OrganisationMetadata.from_dict(data)

    assert actual.practices == expected_practices
    assert actual.sicbls == expected_sicbls


def test_from_dict_returns_list_with_multiple_practices_and_sicbls():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "year": 2020,
        "month": 7,
        "practices": [
            {"ods_code": "A12345", "name": "GP Practice", "asids": ["223456789123"]},
            {"ods_code": "B12345", "name": "GP Practice 2", "asids": ["323456789123"]},
            {"ods_code": "C12345", "name": "GP Practice 3", "asids": ["423456789123"]},
        ],
        "sicbls": [
            {"ods_code": "12A", "name": "SICBL", "practices": ["A12345"]},
            {"ods_code": "34A", "name": "SICBL 2", "practices": ["B12345"]},
            {"ods_code": "56A", "name": "SICBL 3", "practices": ["C12345"]},
        ],
    }

    expected_practices = [
        PracticeMetadata(asids=["223456789123"], ods_code="A12345", name="GP Practice"),
        PracticeMetadata(asids=["323456789123"], ods_code="B12345", name="GP Practice 2"),
        PracticeMetadata(asids=["423456789123"], ods_code="C12345", name="GP Practice 3"),
    ]
    expected_sicbls = [
        SicblMetadata(ods_code="12A", name="SICBL", practices=["A12345"]),
        SicblMetadata(ods_code="34A", name="SICBL 2", practices=["B12345"]),
        SicblMetadata(ods_code="56A", name="SICBL 3", practices=["C12345"]),
    ]
    actual = OrganisationMetadata.from_dict(data)

    assert actual.practices == expected_practices
    assert actual.sicbls == expected_sicbls
