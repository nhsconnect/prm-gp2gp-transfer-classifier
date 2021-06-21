from collections import Counter
from datetime import datetime
from freezegun import freeze_time
from typing import Set, Iterable


from prmdata.domain.data_platform.practice_metrics import (
    construct_practice_summaries,
    IntegratedPracticeMetrics,
    RequesterMetrics,
    MonthlyMetrics,
    PracticeSummary,
)
from tests.builders.gp2gp import build_practice_metrics

A_YEAR = 1890
A_MONTH = 3


def _assert_has_ods_codes(practices: Iterable[PracticeSummary], expected: Set[str]):
    actual_counts = Counter((practice.ods_code for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


def test_has_correct_ods_code_given_a_single_practice():
    sla_metrics = [build_practice_metrics(ods_code="A12345")]

    expected_ods_codes = "A12345"

    actual = construct_practice_summaries(sla_metrics, A_YEAR, A_MONTH)

    assert actual[0].ods_code == expected_ods_codes


def test_has_correct_ods_code_given_two_practices():
    sla_metrics = [
        build_practice_metrics(ods_code="A12345"),
        build_practice_metrics(ods_code="Z56789"),
    ]

    expected_ods_codes = {"A12345", "Z56789"}

    actual = construct_practice_summaries(sla_metrics, year=A_YEAR, month=A_MONTH)

    _assert_has_ods_codes(actual, expected_ods_codes)


def test_has_correct_practice_name_given_a_single_practice():
    sla_metrics = [build_practice_metrics(name="A Practice")]

    expected_name = "A Practice"

    actual = construct_practice_summaries(sla_metrics, A_YEAR, A_MONTH)

    assert actual[0].name == expected_name


def test_has_correct_year_given_a_single_practice():
    sla_metrics = [build_practice_metrics(ods_code="A12345")]

    expected_year = 2019

    actual = construct_practice_summaries(sla_metrics, 2019, A_MONTH)

    assert actual[0].metrics[0].year == expected_year


def test_has_correct_month_given_a_single_practice():
    sla_metrics = [build_practice_metrics(ods_code="A12345")]

    expected_month = 11

    actual = construct_practice_summaries(sla_metrics, A_YEAR, 11)

    assert actual[0].metrics[0].month == expected_month


def test_has_correct_requester_sla_metrics_given_single_practice():
    sla_metrics = [
        build_practice_metrics(transfer_count=3, within_3_days=1, within_8_days=0, beyond_8_days=2)
    ]

    expected_requester_sla_metrics = IntegratedPracticeMetrics(
        transfer_count=3,
        within_3_days_percentage=33.3,
        within_8_days_percentage=0,
        beyond_8_days_percentage=66.7,
    )

    actual = construct_practice_summaries(sla_metrics, A_YEAR, A_MONTH)
    time_to_integrate_sla = actual[0].metrics[0].requester.integrated

    assert time_to_integrate_sla == expected_requester_sla_metrics


@freeze_time(datetime(year=2020, month=1, day=2, hour=23, second=42), tz_offset=0)
def test_has_correct_requester_sla_metrics_given_two_practices():
    sla_metrics = [
        build_practice_metrics(
            ods_code="A12345",
            name="A practice",
            transfer_count=3,
            within_3_days=1,
            within_8_days=0,
            beyond_8_days=2,
        ),
        build_practice_metrics(
            ods_code="Z98765",
            name="Another practice",
            transfer_count=7,
            within_3_days=0,
            within_8_days=5,
            beyond_8_days=2,
        ),
    ]

    expected = [
        PracticeSummary(
            ods_code="A12345",
            name="A practice",
            metrics=[
                MonthlyMetrics(
                    year=2020,
                    month=1,
                    requester=RequesterMetrics(
                        integrated=IntegratedPracticeMetrics(
                            transfer_count=3,
                            within_3_days_percentage=33.3,
                            within_8_days_percentage=0,
                            beyond_8_days_percentage=66.7,
                        ),
                    ),
                )
            ],
        ),
        PracticeSummary(
            ods_code="Z98765",
            name="Another practice",
            metrics=[
                MonthlyMetrics(
                    year=2020,
                    month=1,
                    requester=RequesterMetrics(
                        integrated=IntegratedPracticeMetrics(
                            transfer_count=7,
                            within_3_days_percentage=0,
                            within_8_days_percentage=71.4,
                            beyond_8_days_percentage=28.6,
                        ),
                    ),
                )
            ],
        ),
    ]

    actual = construct_practice_summaries(sla_metrics, 2020, 1)

    assert actual == expected
