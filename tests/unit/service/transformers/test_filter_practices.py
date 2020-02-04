import pytest
from gp2gp.service.transformers import filter_practices
from tests.builders.service import build_practice_sla_metrics

PRACTICE_A = build_practice_sla_metrics(ods="A12345")
ODS_CODE_A = "A12345"
PRACTICE_B = build_practice_sla_metrics(ods="X67890")
ODS_CODE_B = "X67890"
ODS_CODE_C = "P54321"


@pytest.mark.parametrize(
    ["practices", "ods_codes", "expected"],
    [
        ([PRACTICE_A], {ODS_CODE_A}, [PRACTICE_A]),
        ([PRACTICE_A], {ODS_CODE_B}, []),
        ([PRACTICE_A, PRACTICE_B], {ODS_CODE_A}, [PRACTICE_A]),
        ([PRACTICE_A, PRACTICE_B], {ODS_CODE_A, ODS_CODE_B}, [PRACTICE_A, PRACTICE_B]),
        ([PRACTICE_A, PRACTICE_B], {ODS_CODE_A, ODS_CODE_C}, [PRACTICE_A]),
    ],
)
def test_filter_practices(practices, ods_codes, expected):
    actual = filter_practices(practices, ods_codes)
    assert list(actual) == expected
