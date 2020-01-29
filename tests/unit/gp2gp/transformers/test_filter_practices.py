from prmdata.gp2gp.transformers import filter_practices
from tests.builders.gp2gp import build_practice_summary


def test_filter_practices_doesnt_remove_practice_matching_supplied_ods_code():
    ods_codes = {"A12345"}
    practice_summary = build_practice_summary(ods="A12345")
    practices = [practice_summary]

    expected = [practice_summary]

    actual = filter_practices(practices, ods_codes)

    assert list(actual) == expected


def test_filter_practices_removes_practice_not_matching_supplied_ods_code():
    ods_codes = {"A12345"}
    practice_summary = build_practice_summary(ods="X67890")
    practices = [practice_summary]

    expected = []

    actual = filter_practices(practices, ods_codes)

    assert list(actual) == expected


def test_filter_practices_correctly_filters_practices_for_one_ods_code():
    ods_codes = {"A12345"}
    practice_summary_not_matching_ods = build_practice_summary(ods="X67890")
    practice_summary_matching_ods = build_practice_summary(ods="A12345")
    practices = [practice_summary_not_matching_ods, practice_summary_matching_ods]

    expected = [practice_summary_matching_ods]

    actual = filter_practices(practices, ods_codes)

    assert list(actual) == expected


def test_filter_practices_correctly_filters_matching_practices_for_ods_codes():
    ods_codes = {"A12345", "X67890"}
    practice_summary_matching_ods = build_practice_summary(ods="X67890")
    practice_summary_matching_ods_2 = build_practice_summary(ods="A12345")
    practices = [practice_summary_matching_ods, practice_summary_matching_ods_2]

    expected = [practice_summary_matching_ods, practice_summary_matching_ods_2]

    actual = filter_practices(practices, ods_codes)

    assert list(actual) == expected


def test_filter_practices_partially_filters_practices_for_ods_codes():
    ods_codes = {"A12345", "X67890"}
    practice_summary_not_matching_ods = build_practice_summary(ods="P65590")
    practice_summary_matching_ods = build_practice_summary(ods="A12345")
    practices = [practice_summary_not_matching_ods, practice_summary_matching_ods]

    expected = [practice_summary_matching_ods]

    actual = filter_practices(practices, ods_codes)

    assert list(actual) == expected
