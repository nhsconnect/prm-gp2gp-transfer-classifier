from collections import Counter
from datetime import datetime
from typing import Set, Iterable

from freezegun import freeze_time

from gp2gp.dashboard.models import PracticeSummary
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from tests.builders.service import build_practice_summary

A_YEAR = 2019


def _assert_has_ods_codes(practices: Iterable[PracticeSummary], expected: Set[str]):
    actual_counts = Counter((practice.ods for practice in practices))
    expected_counts = Counter(expected)
    assert actual_counts == expected_counts


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42))
def test_dashboard_data_has_correct_generated_on_given_time():
    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42)

    actual = construct_service_dashboard_data(sla_metrics=[], year=A_YEAR)

    assert actual.generated_on == expected_generated_on


@freeze_time(datetime(year=2019, month=4, day=28, hour=23, second=42))
def test_dashboard_data_has_correct_generated_on_given_different_time():
    expected_generated_on = datetime(year=2019, month=4, day=28, hour=23, second=42)

    actual = construct_service_dashboard_data(sla_metrics=[], year=A_YEAR)

    assert actual.generated_on == expected_generated_on


def test_dashboard_data_has_correct_ods_given_a_single_practice():
    practice_summaries = [build_practice_summary(ods="A12345")]

    expected_ods = "A12345"

    actual = construct_service_dashboard_data(practice_summaries, year=A_YEAR)

    assert actual.practices[0].ods == expected_ods


def test_dashboard_data_has_correct_ods_given_two_practices():
    practice_summaries = [
        build_practice_summary(ods="A12345"),
        build_practice_summary(ods="Z56789"),
    ]

    expected_ods = {"A12345", "Z56789"}

    actual = construct_service_dashboard_data(practice_summaries, year=A_YEAR)

    _assert_has_ods_codes(actual.practices, expected_ods)


def test_dashboard_data_has_correct_year_given_a_single_practice():
    practice_summaries = [build_practice_summary(ods="A12345")]

    expected_year = 2019

    actual = construct_service_dashboard_data(practice_summaries, 2019)

    assert actual.practices[0].metrics[0].year == expected_year
