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
    actual = list(calculate_sla_by_practice([transfer]))[0]
    actual_slas = (actual.within_3_days, actual.within_8_days, actual.beyond_8_days)

    expected = (1, 0, 0)

    assert actual_slas == expected
