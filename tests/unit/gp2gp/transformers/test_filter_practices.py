from prmdata.gp2gp.transformers import filter_practices
from tests.builders.gp2gp import build_practice_summary


def test_filter_practices_doesnt_remove_practice_matching_supplied_ods_code():
    ods_code = "A12345"
    ods_codes = {ods_code}
    practice_summary = build_practice_summary(ods="A12345")
    practices = [practice_summary]

    expected = [practice_summary]

    actual = filter_practices(practices, ods_codes)

    assert list(actual) == expected
