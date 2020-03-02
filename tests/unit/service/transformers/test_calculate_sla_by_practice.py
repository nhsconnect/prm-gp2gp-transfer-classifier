from collections import Counter
from datetime import timedelta
from typing import Set, Iterator
import pytest

from gp2gp.odsportal.models import PracticeDetails
from gp2gp.service.models import PracticeSlaMetrics
from gp2gp.service.transformers import calculate_sla_by_practice
from tests.builders.common import a_string
from tests.builders.service import build_transfer


def _assert_has_ods_codes(practices: Iterator[PracticeSlaMetrics], expected: Set[str]):
    actual_counts = Counter((practice.ods_code for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


def _assert_first_summary_has_sla_counts(
    practices: Iterator[PracticeSlaMetrics],
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


def test_groups_by_ods_code_given_single_practice_and_single_transfer():
    practices = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfers = [build_transfer(requesting_practice_ods_code="A12345")]

    actual = calculate_sla_by_practice(practices, transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_groups_by_ods_code_given_single_practice_and_no_transfers():
    practices = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfers = []

    actual = calculate_sla_by_practice(practices, transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_warns_about_transfer_with_unexpected_ods_code():
    practices = []
    transfers = [build_transfer(requesting_practice_ods_code="A12345")]

    with pytest.warns(RuntimeWarning):
        calculate_sla_by_practice(practices, transfers)


def test_groups_by_ods_code_given_two_practices_and_two_transfers_from_different_practices():
    practices = [
        PracticeDetails(ods_code="A12345", name=a_string()),
        PracticeDetails(ods_code="X67890", name=a_string()),
    ]
    transfers = [
        build_transfer(requesting_practice_ods_code="A12345"),
        build_transfer(requesting_practice_ods_code="X67890"),
    ]

    actual = calculate_sla_by_practice(practices, transfers)

    _assert_has_ods_codes(actual, {"A12345", "X67890"})


def test_groups_by_ods_code_given_single_practice_and_transfers_from_the_same_practice():
    practices = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfers = [
        build_transfer(requesting_practice_ods_code="A12345"),
        build_transfer(requesting_practice_ods_code="A12345"),
    ]

    actual = calculate_sla_by_practice(practices, transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_contains_practice_name():
    expected_name = "A Practice"
    practices = [PracticeDetails(ods_code=a_string(), name=expected_name)]
    transfers = []

    actual_name = list(calculate_sla_by_practice(practices, transfers))[0].name

    assert actual_name == expected_name


def test_returns_practice_sla_metrics_placeholder_given_a_list_with_one_practice_and_no_metrics():
    practice_list = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfers = []
    actual = calculate_sla_by_practice(practice_list, transfers)

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=0, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_within_3_days():
    practice_list = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfer = build_transfer(
        requesting_practice_ods_code="A12345", sla_duration=timedelta(hours=1, minutes=10)
    )
    actual = calculate_sla_by_practice(practice_list, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=1, within_8_days=0, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_within_8_days():
    practice_list = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfer = build_transfer(
        requesting_practice_ods_code="A12345", sla_duration=timedelta(days=7, hours=1, minutes=10)
    )
    actual = calculate_sla_by_practice(practice_list, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=1, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_beyond_8_days():
    practice_list = [PracticeDetails(ods_code="A12345", name=a_string())]
    transfer = build_transfer(
        requesting_practice_ods_code="A12345", sla_duration=timedelta(days=8, hours=1, minutes=10)
    )
    actual = calculate_sla_by_practice(practice_list, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=0, beyond_8_days=1)


def test_calculate_sla_by_practice_calculates_sla_given_transfers_for_2_practices():
    practice_list = [
        PracticeDetails(ods_code="A12345", name="A Practice"),
        PracticeDetails(ods_code="B12345", name="Another Practice"),
    ]
    transfers = [
        build_transfer(
            requesting_practice_ods_code="A12345",
            sla_duration=timedelta(days=8, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_ods_code="B12345",
            sla_duration=timedelta(days=4, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_ods_code="A12345",
            sla_duration=timedelta(days=0, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_ods_code="B12345",
            sla_duration=timedelta(days=8, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_ods_code="B12345",
            sla_duration=timedelta(days=5, hours=1, minutes=10),
        ),
    ]

    expected = [
        PracticeSlaMetrics(
            ods_code="A12345", name="A Practice", within_3_days=1, within_8_days=0, beyond_8_days=1
        ),
        PracticeSlaMetrics(
            ods_code="B12345",
            name="Another Practice",
            within_3_days=0,
            within_8_days=2,
            beyond_8_days=1,
        ),
    ]

    actual = calculate_sla_by_practice(practice_list, transfers)
    actual_sorted = sorted(actual, key=lambda p: p.ods_code)

    assert actual_sorted == expected
