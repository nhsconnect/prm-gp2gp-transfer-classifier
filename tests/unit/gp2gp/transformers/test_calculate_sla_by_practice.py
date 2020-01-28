from datetime import timedelta

from prmdata.gp2gp.transformers import calculate_sla_by_practice
from tests.builders.gp2gp import build_transfer


def test_calculate_sla_by_practice_given_single_transfer():
    transfer = build_transfer(requesting_practice_ods="A12345")

    actual = calculate_sla_by_practice([transfer])
    actual_ods = [practice.ods for practice in actual]
    expected = ["A12345"]

    assert actual_ods == expected


def test_calculate_sla_by_practice_returns_correct_practice_sla_summary_for_one_transfer():
    transfer = build_transfer(sla_duration=timedelta(hours=1, minutes=10))

    actual = list(calculate_sla_by_practice([transfer]))
    actual_within_3_day_count = actual[0].within_3_days
    expected = 1

    assert actual_within_3_day_count == expected
