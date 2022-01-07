from datetime import datetime

import pytest
from dateutil.tz import tzutc

from prmdata.domain.datetime import MonthlyReportingWindow


def test_prior_to_correctly_determines_metric_month():
    moment = datetime(2021, 3, 4)

    reporting_window = MonthlyReportingWindow.prior_to(moment)

    expected = (2021, 2)

    actual = reporting_window.metric_month

    assert actual == expected


def test_prior_to_correctly_determines_metric_month_over_new_year():
    moment = datetime(2021, 1, 4)

    reporting_window = MonthlyReportingWindow.prior_to(moment)

    expected = (2020, 12)

    actual = reporting_window.metric_month

    assert actual == expected


def test_prior_to_correctly_determines_overflow_month():
    moment = datetime(2021, 3, 4)

    reporting_window = MonthlyReportingWindow.prior_to(moment)

    expected = (2021, 3)

    actual = reporting_window.overflow_month

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        ({"date": datetime(2021, 1, 31, tzinfo=tzutc()), "expected": False}),
        ({"date": datetime(2021, 2, 1, tzinfo=tzutc()), "expected": True}),
        ({"date": datetime(2021, 2, 20, tzinfo=tzutc()), "expected": True}),
        ({"date": datetime(2021, 2, 28, tzinfo=tzutc()), "expected": True}),
        ({"date": datetime(2021, 3, 1, tzinfo=tzutc()), "expected": False}),
    ],
)
def test_contains_returns_correct_boolean(test_case):
    moment = datetime(2021, 3, 4)

    reporting_window = MonthlyReportingWindow.prior_to(moment)

    actual = reporting_window.contains(test_case["date"])

    assert actual == test_case["expected"]
