from prmdata.domain.gp2gp.practice_lookup import PracticeLookup
from tests.builders.ods_portal import build_practice_details


def test_all_practices_returns_nothing_given_no_practices():
    practices = []
    practice_lookup = PracticeLookup(practices)

    expected = []

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
