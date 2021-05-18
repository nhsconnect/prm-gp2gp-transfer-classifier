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
