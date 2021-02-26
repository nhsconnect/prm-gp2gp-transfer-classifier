from datetime import datetime

from gp2gp.domain.dashboard.organisation_metadata import (
    construct_organisation_metadata,
    OrganisationDetails,
)
from gp2gp.domain.odsportal.models import OrganisationMetadata, CcgDetails, PracticeDetails


def test_maps_generated_on():
    organisation_metadata = OrganisationMetadata(
        generated_on=datetime(2020, 1, 1),
        practices=[],
        ccgs=[],
    )

    expected_datetime = datetime(2020, 1, 1)

    actual = construct_organisation_metadata(organisation_metadata)

    assert actual.generated_on == expected_datetime


def test_maps_ccg_details_to_organisation_details():
    organisation_metadata = OrganisationMetadata(
        generated_on=datetime(2020, 1, 1),
        practices=[],
        ccgs=[CcgDetails(ods_code="12X", name="A CCG")],
    )

    expected_ccgs = [OrganisationDetails(ods_code="12X", name="A CCG")]

    actual = construct_organisation_metadata(organisation_metadata)

    assert actual.ccgs == expected_ccgs


def test_maps_multiple_ccg_details_to_organisation_details():
    organisation_metadata = OrganisationMetadata(
        generated_on=datetime(2020, 1, 1),
        practices=[],
        ccgs=[
            CcgDetails(ods_code="12X", name="A CCG"),
            CcgDetails(ods_code="11E", name="A CCG 2"),
            CcgDetails(ods_code="10P", name="A CCG 3"),
        ],
    )

    expected_ccgs = [
        OrganisationDetails(ods_code="12X", name="A CCG"),
        OrganisationDetails(ods_code="11E", name="A CCG 2"),
        OrganisationDetails(ods_code="10P", name="A CCG 3"),
    ]

    actual = construct_organisation_metadata(organisation_metadata)

    assert actual.ccgs == expected_ccgs


def test_maps_practice_details_to_organisation_details():
    organisation_metadata = OrganisationMetadata(
        generated_on=datetime(2020, 1, 1),
        practices=[PracticeDetails(ods_code="A12345", name="A Practice", asids=["123456789876"])],
        ccgs=[],
    )

    expected_practices = [OrganisationDetails(ods_code="A12345", name="A Practice")]

    actual = construct_organisation_metadata(organisation_metadata)

    assert actual.practices == expected_practices


def test_maps_multiple_practice_details_to_organisation_details():
    organisation_metadata = OrganisationMetadata(
        generated_on=datetime(2020, 1, 1),
        practices=[
            PracticeDetails(ods_code="A12345", name="A Practice", asids=["123456789876"]),
            PracticeDetails(ods_code="B12345", name="A Practice 2", asids=["121456789876"]),
            PracticeDetails(ods_code="C12345", name="A Practice 3", asids=["165456789876"]),
        ],
        ccgs=[],
    )

    expected_practices = [
        OrganisationDetails(ods_code="A12345", name="A Practice"),
        OrganisationDetails(ods_code="B12345", name="A Practice 2"),
        OrganisationDetails(ods_code="C12345", name="A Practice 3"),
    ]

    actual = construct_organisation_metadata(organisation_metadata)

    assert actual.practices == expected_practices
