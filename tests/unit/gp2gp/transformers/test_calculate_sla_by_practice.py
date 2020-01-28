from collections import Counter
from datetime import timedelta
from typing import Set, Iterable

from prmdata.gp2gp.models import PracticeSlaSummary
from prmdata.gp2gp.transformers import calculate_sla_by_practice
from tests.builders.gp2gp import build_transfer


def _assert_has_ods_codes(practices: Iterable[PracticeSlaSummary], expected: Set[str]):
    actual_counts = Counter((practice.ods for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


def test_calculate_sla_by_practice_given_single_transfer():
    transfers = [build_transfer(requesting_practice_ods="A12345")]

    actual = calculate_sla_by_practice(transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_calculate_sla_by_practice_returns_correct_practice_sla_summary_for_one_transfer():
    transfer = build_transfer(sla_duration=timedelta(hours=1, minutes=10))

    actual = list(calculate_sla_by_practice([transfer]))
    actual_within_3_day_count = actual[0].within_3_days
    expected = 1

    assert actual_within_3_day_count == expected
