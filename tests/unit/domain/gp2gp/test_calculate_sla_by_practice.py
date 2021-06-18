from collections import Counter
from datetime import timedelta
from typing import Set, Iterator, List

import pytest

from prmdata.domain.gp2gp.practice_lookup import PracticeLookup
from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.ods_portal.models import PracticeDetails
from prmdata.domain.gp2gp.practice_metrics import (
    PracticeMetrics,
    calculate_sla_by_practice,
    IntegratedPracticeMetrics,
)

from tests.builders.common import a_string
from tests.builders.gp2gp import build_transfer


def _assert_has_ods_codes(practices: Iterator[PracticeMetrics], expected: Set[str]):
    actual_counts = Counter((practice.ods_code for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


def _assert_first_summary_has_sla_counts(
    practices: Iterator[PracticeMetrics],
    within_3_days: int,
    within_8_days: int,
    beyond_8_days: int,
):
    first_summary = next(practices)

    expected_slas = (within_3_days, within_8_days, beyond_8_days)

    actual_slas = (
        first_summary.integrated.within_3_days,
        first_summary.integrated.within_8_days,
        first_summary.integrated.beyond_8_days,
    )

    assert actual_slas == expected_slas


def test_groups_by_ods_code_given_single_practice_and_single_transfer():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfers = [build_transfer(requesting_practice_asid="121212121212")]

    actual = calculate_sla_by_practice(lookup, transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_groups_by_ods_code_given_single_practice_and_no_transfers():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfers: List[Transfer] = []

    actual = calculate_sla_by_practice(lookup, transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_warns_about_transfer_with_unexpected_asid():
    lookup = PracticeLookup([])
    transfers = [build_transfer(requesting_practice_asid="121212121212")]

    with pytest.warns(RuntimeWarning):
        calculate_sla_by_practice(lookup, transfers)


def test_groups_by_asid_given_two_practices_and_two_transfers_from_different_practices():
    lookup = PracticeLookup(
        [
            PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string()),
            PracticeDetails(asids=["343434343434"], ods_code="X67890", name=a_string()),
        ]
    )
    transfers = [
        build_transfer(requesting_practice_asid="121212121212"),
        build_transfer(requesting_practice_asid="343434343434"),
    ]

    actual = calculate_sla_by_practice(lookup, transfers)

    _assert_has_ods_codes(actual, {"A12345", "X67890"})


def test_groups_by_asid_given_single_practice_and_transfers_from_the_same_practice():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfers = [
        build_transfer(requesting_practice_asid="121212121212"),
        build_transfer(requesting_practice_asid="121212121212"),
    ]

    actual = calculate_sla_by_practice(lookup, transfers)

    _assert_has_ods_codes(actual, {"A12345"})


def test_contains_practice_name():
    expected_name = "A Practice"
    lookup = PracticeLookup(
        [PracticeDetails(asids=[a_string()], ods_code=a_string(), name=expected_name)]
    )
    transfers: List[Transfer] = []

    actual_name = list(calculate_sla_by_practice(lookup, transfers))[0].name

    assert actual_name == expected_name


def test_returns_practice_sla_metrics_placeholder_given_a_list_with_one_practice_and_no_metrics():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfers: List[Transfer] = []

    actual = calculate_sla_by_practice(lookup, transfers)

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=0, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_within_3_days():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfer = build_transfer(
        requesting_practice_asid="121212121212", sla_duration=timedelta(hours=1, minutes=10)
    )
    actual = calculate_sla_by_practice(lookup, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=1, within_8_days=0, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_within_8_days():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfer = build_transfer(
        requesting_practice_asid="121212121212", sla_duration=timedelta(days=7, hours=1, minutes=10)
    )
    actual = calculate_sla_by_practice(lookup, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=1, beyond_8_days=0)


def test_calculate_sla_by_practice_calculates_sla_given_one_transfer_beyond_8_days():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfer = build_transfer(
        requesting_practice_asid="121212121212", sla_duration=timedelta(days=8, hours=1, minutes=10)
    )
    actual = calculate_sla_by_practice(lookup, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=0, within_8_days=0, beyond_8_days=1)


def test_calculate_sla_by_practice_calculates_sla_given_transfers_for_2_practices():
    lookup = PracticeLookup(
        [
            PracticeDetails(asids=["121212121212"], ods_code="A12345", name="A Practice"),
            PracticeDetails(asids=["343434343434"], ods_code="B12345", name="Another Practice"),
        ]
    )
    transfers = [
        build_transfer(
            requesting_practice_asid="121212121212",
            sla_duration=timedelta(days=8, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="343434343434",
            sla_duration=timedelta(days=4, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="121212121212",
            sla_duration=timedelta(days=0, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="343434343434",
            sla_duration=timedelta(days=8, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="343434343434",
            sla_duration=timedelta(days=5, hours=1, minutes=10),
        ),
    ]

    expected = [
        PracticeMetrics(
            ods_code="A12345",
            name="A Practice",
            integrated=IntegratedPracticeMetrics(
                transfer_count=2, within_3_days=1, within_8_days=0, beyond_8_days=1
            ),
        ),
        PracticeMetrics(
            ods_code="B12345",
            name="Another Practice",
            integrated=IntegratedPracticeMetrics(
                transfer_count=3, within_3_days=0, within_8_days=2, beyond_8_days=1
            ),
        ),
    ]

    actual = calculate_sla_by_practice(lookup, transfers)
    actual_sorted = sorted(actual, key=lambda p: p.ods_code)

    assert actual_sorted == expected


def test_counts_second_asid_for_practice_with_two_asids():
    lookup = PracticeLookup(
        [
            PracticeDetails(
                asids=["121212121212", "343434343434"], ods_code="A12345", name=a_string()
            )
        ]
    )
    transfer = build_transfer(
        requesting_practice_asid="343434343434",
        sla_duration=timedelta(hours=1, minutes=10),
    )
    actual = calculate_sla_by_practice(lookup, [transfer])

    _assert_first_summary_has_sla_counts(actual, within_3_days=1, within_8_days=0, beyond_8_days=0)


def test_counts_both_asids_for_practice_with_two_asids():
    lookup = PracticeLookup(
        [
            PracticeDetails(
                asids=["121212121212", "343434343434"], ods_code="A12345", name=a_string()
            )
        ]
    )
    transfers = [
        build_transfer(
            requesting_practice_asid="343434343434",
            sla_duration=timedelta(hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="121212121212",
            sla_duration=timedelta(days=5, hours=1, minutes=10),
        ),
    ]
    actual = calculate_sla_by_practice(lookup, transfers)

    _assert_first_summary_has_sla_counts(actual, within_3_days=1, within_8_days=1, beyond_8_days=0)


def test_returns_sum_of_all_integrated_transfers():
    lookup = PracticeLookup(
        [PracticeDetails(asids=["121212121212"], ods_code="A12345", name=a_string())]
    )
    transfers = [
        build_transfer(
            requesting_practice_asid="121212121212",
            sla_duration=timedelta(days=0, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="121212121212",
            sla_duration=timedelta(days=7, hours=1, minutes=10),
        ),
        build_transfer(
            requesting_practice_asid="121212121212",
            sla_duration=timedelta(days=10, hours=1, minutes=10),
        ),
    ]
    actual = list(calculate_sla_by_practice(lookup, transfers))

    assert actual[0].integrated.transfer_count == 3
