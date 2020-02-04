from collections import Counter
from datetime import datetime
from typing import Set, Iterable

from dateutil.tz.tz import tzutc
from freezegun import freeze_time

from gp2gp.dashboard.models import (
    PracticeSummary,
    TimeToIntegrateSla,
    MonthlyMetrics,
    RequesterMetrics,
    ServiceDashboardData,
)
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from tests.builders.service import build_practice_sla_metrics

A_YEAR = 1890
A_MONTH = 3


def _assert_has_ods_codes(practices: Iterable[PracticeSummary], expected: Set[str]):
    actual_counts = Counter((practice.ods for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42), tz_offset=0)
def test_dashboard_data_has_correct_generated_on_given_time():
    sla_metrics = []

    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42, tzinfo=tzutc())

    actual = construct_service_dashboard_data(sla_metrics, A_YEAR, A_MONTH)

    assert actual.generated_on == expected_generated_on


def test_dashboard_data_has_correct_ods_given_a_single_practice():
    sla_metrics = [build_practice_sla_metrics(ods="A12345")]

    expected_ods = "A12345"

    actual = construct_service_dashboard_data(sla_metrics, A_YEAR, A_MONTH)

    assert actual.practices[0].ods == expected_ods


def test_dashboard_data_has_correct_ods_given_two_practices():
    sla_metrics = [
        build_practice_sla_metrics(ods="A12345"),
        build_practice_sla_metrics(ods="Z56789"),
    ]

    expected_ods = {"A12345", "Z56789"}

    actual = construct_service_dashboard_data(sla_metrics, year=A_YEAR, month=A_MONTH)

    _assert_has_ods_codes(actual.practices, expected_ods)


def test_dashboard_data_has_correct_year_given_a_single_practice():
    sla_metrics = [build_practice_sla_metrics(ods="A12345")]

    expected_year = 2019

    actual = construct_service_dashboard_data(sla_metrics, 2019, A_MONTH)

    assert actual.practices[0].metrics[0].year == expected_year


def test_dashboard_data_has_correct_month_given_a_single_practice():
    sla_metrics = [build_practice_sla_metrics(ods="A12345")]

    expected_month = 11

    actual = construct_service_dashboard_data(sla_metrics, A_YEAR, 11)

    assert actual.practices[0].metrics[0].month == expected_month


def test_dashboard_data_has_correct_requester_sla_metrics_given_single_practice():
    sla_metrics = [build_practice_sla_metrics(within_3_days=1, within_8_days=0, beyond_8_days=2)]

    expected_requester_sla_metrics = TimeToIntegrateSla(
        within_3_days=1, within_8_days=0, beyond_8_days=2
    )

    actual = construct_service_dashboard_data(sla_metrics, A_YEAR, A_MONTH)
    time_to_integrate_sla = actual.practices[0].metrics[0].requester.time_to_integrate_sla

    assert time_to_integrate_sla == expected_requester_sla_metrics


@freeze_time(datetime(year=2020, month=1, day=2, hour=23, second=42), tz_offset=0)
def test_dashboard_data_has_correct_requester_sla_metrics_given_two_practices():
    sla_metrics = [
        build_practice_sla_metrics(ods="A12345", within_3_days=1, within_8_days=0, beyond_8_days=2),
        build_practice_sla_metrics(ods="Z98765", within_3_days=0, within_8_days=5, beyond_8_days=2),
    ]

    expected = ServiceDashboardData(
        generated_on=datetime(year=2020, month=1, day=2, hour=23, second=42, tzinfo=tzutc()),
        practices=[
            PracticeSummary(
                ods="A12345",
                metrics=[
                    MonthlyMetrics(
                        year=2020,
                        month=1,
                        requester=RequesterMetrics(
                            time_to_integrate_sla=TimeToIntegrateSla(
                                within_3_days=1, within_8_days=0, beyond_8_days=2
                            )
                        ),
                    )
                ],
            ),
            PracticeSummary(
                ods="Z98765",
                metrics=[
                    MonthlyMetrics(
                        year=2020,
                        month=1,
                        requester=RequesterMetrics(
                            time_to_integrate_sla=TimeToIntegrateSla(
                                within_3_days=0, within_8_days=5, beyond_8_days=2
                            )
                        ),
                    )
                ],
            ),
        ],
    )

    actual = construct_service_dashboard_data(sla_metrics, 2020, 1)

    assert actual == expected
