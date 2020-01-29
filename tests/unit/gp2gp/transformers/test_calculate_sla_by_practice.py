from collections import Counter
from datetime import timedelta
from typing import Set, Iterator

from prmdata.gp2gp.models import PracticeSlaSummary
from prmdata.gp2gp.transformers import calculate_sla_by_practice
from tests.builders.gp2gp import build_transfer


def _assert_has_ods_codes(practices: Iterator[PracticeSlaSummary], expected: Set[str]):
    actual_counts = Counter((practice.ods for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


def _assert_first_summary_has_sla_counts(
    practices: Iterator[PracticeSlaSummary],
    within_3_days: int,
    within_8_days: int,
    beyond_8_days: int,
):
    first_summary = next(practices)

    expected_slas = (within_3_days, within_8_days, beyond_8_days)

    actual_slas = (
        first_summary.within_3_days,
        first_summary.within_8_days,
        first_summary.beyond_8_days,
    )

    assert actual_slas == expected_slas


def test_calculate_sla_by_practice_groups_by_ods_given_single_transfer():
    transfers = [build_transfer(requesting_practice_ods="A12345")]

    actual = calculate_sla_by_practice(transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_calculate_sla_by_practice_groups_by_ods_given_two_transfers_from_different_practices():
    transfers = [
        build_transfer(requesting_practice_ods="A12345"),
        build_transfer(requesting_practice_ods="X67890"),
    ]

    actual = calculate_sla_by_practice(transfers)

    _assert_has_ods_codes(actual, {"A12345", "X67890"})


def test_calculate_sla_by_practice_groups_by_ods_given_two_transfers_from_the_same_practice():
    transfers = [
        build_transfer(requesting_practice_ods="A12345"),
        build_transfer(requesting_practice_ods="A12345"),
    ]

    actual = calculate_sla_by_practice(transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_within_3_days():
    transfer = build_transfer(sla_duration=timedelta(hours=1, minutes=10))
    actual = calculate_sla_by_practice([transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=1, within_8_days=0, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_within_8_days():
    transfer = build_transfer(sla_duration=timedelta(days=7, hours=1, minutes=10))
    actual = calculate_sla_by_practice([transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=1, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_beyond_8_days():
    transfer = build_transfer(sla_duration=timedelta(days=8, hours=1, minutes=10))
    actual = calculate_sla_by_practice([transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=0, beyond_8_days=1)


def test_calculate_sla_by_practice_calculates_sla_given_transfers_for_2_practices():
    transfers = [
        build_transfer(
            requesting_practice_ods="A12345", sla_duration=timedelta(days=8, hours=1, minutes=10)
        ),
        build_transfer(
            requesting_practice_ods="B12345", sla_duration=timedelta(days=4, hours=1, minutes=10)
        ),
        build_transfer(
            requesting_practice_ods="A12345", sla_duration=timedelta(days=0, hours=1, minutes=10)
        ),
        build_transfer(
            requesting_practice_ods="B12345", sla_duration=timedelta(days=8, hours=1, minutes=10)
        ),
        build_transfer(
            requesting_practice_ods="B12345", sla_duration=timedelta(days=5, hours=1, minutes=10)
        ),
    ]

    expected = [
        PracticeSlaSummary(ods="A12345", within_3_days=1, within_8_days=0, beyond_8_days=1),
        PracticeSlaSummary(ods="B12345", within_3_days=0, within_8_days=2, beyond_8_days=1),
    ]

    actual = calculate_sla_by_practice(transfers)
    actual_sorted = sorted(actual, key=lambda p: p.ods)

    assert actual_sorted == expected
