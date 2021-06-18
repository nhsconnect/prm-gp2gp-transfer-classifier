from typing import List

from prmdata.domain.gp2gp.practice_lookup import PracticeLookup
from prmdata.domain.ods_portal.models import PracticeDetails

from tests.builders.ods_portal import build_practice_details


def test_all_practices_returns_nothing_given_no_practices():
    practices: List[PracticeDetails] = []
    practice_lookup = PracticeLookup(practices)

    expected: List[PracticeDetails] = []

    actual = list(practice_lookup.all_practices())

    assert actual == expected


def test_all_practices_returns_practices():
    practice_one = build_practice_details()
    practice_two = build_practice_details()

    practices = [practice_one, practice_two]

    practice_lookup = PracticeLookup(practices)

    expected = [practice_one, practice_two]

    actual = list(practice_lookup.all_practices())

    assert actual == expected


def test_has_asid_code_returns_false_given_no_matching_practice():
    practices = [build_practice_details(asids=["123"])]

    practice_lookup = PracticeLookup(practices)

    expected = False

    actual = practice_lookup.has_asid_code("456")

    assert actual == expected


def test_has_asid_code_returns_true_given_a_matching_practice():
    practices = [build_practice_details(asids=["123"])]

    practice_lookup = PracticeLookup(practices)

    expected = True

    actual = practice_lookup.has_asid_code("123")

    assert actual == expected


def test_has_asid_code_returns_true_given_a_matching_practice_with_multiple_asid():
    practices = [build_practice_details(asids=["123", "456"])]

    practice_lookup = PracticeLookup(practices)

    expected = True

    actual = practice_lookup.has_asid_code("456")

    assert actual == expected


def test_has_asid_code_returns_true_given_multiple_practices():
    practices = [build_practice_details(asids=["123"]), build_practice_details(asids=["456"])]

    practice_lookup = PracticeLookup(practices)

    expected = True

    actual = practice_lookup.has_asid_code("456")

    assert actual == expected


def test_ods_code_from_asid_returns_none_given_no_practices():
    practices: List[PracticeDetails] = []
    practice_lookup = PracticeLookup(practices)

    expected = None

    actual = practice_lookup.ods_code_from_asid("123")

    assert actual == expected


def test_ods_code_from_asid_returns_matching_practice_given_practice_with_a_single_asid():
    practice = build_practice_details(asids=["123"], ods_code="ABC")
    practice_lookup = PracticeLookup([practice])

    expected = "ABC"

    actual = practice_lookup.ods_code_from_asid("123")

    assert actual == expected


def test_ods_code_from_asid_returns_matching_practice_given_practice_with_multiple_asids():
    practice = build_practice_details(asids=["123", "456"], ods_code="ABC")
    practice_lookup = PracticeLookup([practice])

    expected = "ABC"

    actual = practice_lookup.ods_code_from_asid("456")

    assert actual == expected


def test_ods_code_from_asid_returns_matching_practice_given_multiple_practices():
    practice_one = build_practice_details(asids=["123"])
    practice_two = build_practice_details(asids=["456"], ods_code="ABC")
    practice_lookup = PracticeLookup([practice_one, practice_two])

    expected = "ABC"

    actual = practice_lookup.ods_code_from_asid("456")

    assert actual == expected


def test_all_ods_codes_returns_nothing_given_no_practices():
    practices: List[PracticeDetails] = []
    practice_lookup = PracticeLookup(practices)

    expected: List[PracticeDetails] = []

    actual = list(practice_lookup.all_ods_codes())

    assert actual == expected


def test_all_ods_codes_returns_ods_code_given_a_practice():
    practices = [build_practice_details(ods_code="ABC")]
    practice_lookup = PracticeLookup(practices)

    expected = ["ABC"]

    actual = list(practice_lookup.all_ods_codes())

    assert actual == expected


def test_all_ods_codes_returns_ods_code_given_multiple_practices():
    practices = [build_practice_details(ods_code="ABC"), build_practice_details(ods_code="DEF")]
    practice_lookup = PracticeLookup(practices)

    expected = ["ABC", "DEF"]

    actual = list(practice_lookup.all_ods_codes())

    assert actual == expected
