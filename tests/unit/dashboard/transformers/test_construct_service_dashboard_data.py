from collections import Counter
from datetime import datetime
from typing import Set, Iterable

from freezegun import freeze_time

from gp2gp.dashboard.models import PracticeSummary
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from tests.builders.service import build_practice_sla_metrics

A_YEAR = 1890
A_MONTH = 3


def _assert_has_ods_codes(practices: Iterable[PracticeSummary], expected: Set[str]):
    actual_counts = Counter((practice.ods for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42))
def test_dashboard_data_has_correct_generated_on_given_time():
    sla_metrics = []

    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42)

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
