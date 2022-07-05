from typing import List

from prmdata.domain.ods_portal.organisation_lookup import OrganisationLookup
from prmdata.domain.ods_portal.organisation_metadata import PracticeMetadata
from tests.builders.common import a_string
from tests.builders.ods_portal import build_icb_metadata, build_practice_metadata


def test_year_property_returns_the_year_of_data():
    organisation_lookup = OrganisationLookup(practices=[], icbs=[], year_month=(2020, 1))

    expected = 2020

    actual = organisation_lookup.year

    assert actual == expected


def test_month_property_returns_the_month_of_data():
    organisation_lookup = OrganisationLookup(practices=[], icbs=[], year_month=(2020, 1))

    expected = 1

    actual = organisation_lookup.month

    assert actual == expected


def test_has_asid_code_returns_false_given_no_matching_practice():
    practices = [build_practice_metadata(asids=["123"])]

    organisation_lookup = OrganisationLookup(practices, icbs=[], year_month=(2020, 1))

    expected = False

    actual = organisation_lookup.has_asid_code("456")

    assert actual == expected


def test_has_asid_code_returns_true_given_a_matching_practice():
    practices = [build_practice_metadata(asids=["123"])]

    organisation_lookup = OrganisationLookup(practices, icbs=[], year_month=(2020, 1))

    expected = True

    actual = organisation_lookup.has_asid_code("123")

    assert actual == expected


def test_has_asid_code_returns_true_given_a_matching_practice_with_multiple_asid():
    practices = [build_practice_metadata(asids=["123", "456"])]

    organisation_lookup = OrganisationLookup(practices, icbs=[], year_month=(2020, 1))

    expected = True

    actual = organisation_lookup.has_asid_code("456")

    assert actual == expected


def test_has_asid_code_returns_true_given_multiple_practices():
    practices = [build_practice_metadata(asids=["123"]), build_practice_metadata(asids=["456"])]

    organisation_lookup = OrganisationLookup(practices, icbs=[], year_month=(2020, 1))

    expected = True

    actual = organisation_lookup.has_asid_code("456")

    assert actual == expected


def test_practice_ods_code_from_asid_returns_none_given_no_practices():
    practices: List[PracticeMetadata] = []
    organisation_lookup = OrganisationLookup(practices, icbs=[], year_month=(2020, 1))

    expected = None

    actual = organisation_lookup.practice_ods_code_from_asid("123")

    assert actual == expected


def test_practice_ods_code_from_asid_returns_matching_practice_given_practice_with_a_single_asid():
    practice = build_practice_metadata(asids=["123"], ods_code="ABC")
    organisation_lookup = OrganisationLookup(practices=[practice], icbs=[], year_month=(2020, 1))

    expected = "ABC"

    actual = organisation_lookup.practice_ods_code_from_asid("123")

    assert actual == expected


def test_practice_ods_code_from_asid_returns_matching_practice_given_practice_with_multiple_asids():
    practice = build_practice_metadata(asids=["123", "456"], ods_code="ABC")
    organisation_lookup = OrganisationLookup(practices=[practice], icbs=[], year_month=(2020, 1))

    expected = "ABC"

    actual = organisation_lookup.practice_ods_code_from_asid("456")

    assert actual == expected


def test_practice_ods_code_from_asid_returns_matching_practice_given_multiple_practices():
    practice_one = build_practice_metadata(asids=["123"])
    practice_two = build_practice_metadata(asids=["456"], ods_code="ABC")
    organisation_lookup = OrganisationLookup(
        practices=[practice_one, practice_two], icbs=[], year_month=(2020, 1)
    )

    expected = "ABC"

    actual = organisation_lookup.practice_ods_code_from_asid("456")

    assert actual == expected


def test_returns_practice_name_from_asid():
    practice_one = build_practice_metadata(asids=["123"])
    practice_two = build_practice_metadata(asids=["456"], ods_code="ABC", name="Practice 2")
    organisation_lookup = OrganisationLookup(
        practices=[practice_one, practice_two], icbs=[], year_month=(2020, 1)
    )

    expected = practice_two.name

    actual = organisation_lookup.practice_name_from_asid("456")

    assert actual == expected


def test_returns_icb_name_from_ods_code():
    practice = build_practice_metadata(asids=["456"], ods_code="A123", name="Practice 2")
    icb = build_icb_metadata(practices=["A123"], ods_code="12A", name="A ICB")
    organisation_lookup = OrganisationLookup(practices=[practice], icbs=[icb], year_month=(2020, 1))

    expected = icb.name

    actual = organisation_lookup.icb_name_from_practice_ods_code("A123")

    assert actual == expected


def test_icb_ods_code_from_practice_ods_code_returns_none_given_no_icbs():
    organisation_lookup = OrganisationLookup(practices=[], icbs=[], year_month=(2020, 1))

    expected = None

    actual = organisation_lookup.icb_ods_code_from_practice_ods_code("A123")

    assert actual == expected


def test_icb_ods_code_from_practice_ods_code_returns_matching_icb():
    icb = build_icb_metadata(practices=["A123"], ods_code="12A")
    organisation_lookup = OrganisationLookup(practices=[], icbs=[icb], year_month=(2020, 1))

    expected = "12A"

    actual = organisation_lookup.icb_ods_code_from_practice_ods_code("A123")

    assert actual == expected


def test_icb_ods_code_from_practice_ods_code_returns_matching_icb_with_multiple_practices():
    icb = build_icb_metadata(practices=["B3432", a_string(), a_string()], ods_code="3W")
    organisation_lookup = OrganisationLookup(practices=[], icbs=[icb], year_month=(2020, 1))

    expected = "3W"

    actual = organisation_lookup.icb_ods_code_from_practice_ods_code("B3432")

    assert actual == expected


def test_icb_ods_code_from_practice_ods_code_returns_matching_icb_given_multiple_icbs():
    icb = build_icb_metadata(practices=["A2431"], ods_code="42C")
    organisation_lookup = OrganisationLookup(
        practices=[], icbs=[build_icb_metadata(), build_icb_metadata(), icb], year_month=(2020, 1)
    )

    expected = "42C"

    actual = organisation_lookup.icb_ods_code_from_practice_ods_code("A2431")

    assert actual == expected
