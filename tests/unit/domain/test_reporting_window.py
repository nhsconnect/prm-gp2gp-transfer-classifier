from datetime import datetime, timedelta

import pytest
from dateutil.tz import tzutc

from prmdata.domain.reporting_window import ReportingWindow


def test_get_dates_returns_list_of_datetimes_within_reporting_window():
    start_datetime = datetime(year=2021, month=12, day=30)
    end_datetime = datetime(year=2022, month=1, day=3)
    conversation_cutoff = timedelta(days=14)

    reporting_window = ReportingWindow(start_datetime, end_datetime, conversation_cutoff)

    expected = [
        datetime(year=2021, month=12, day=30),
        datetime(year=2021, month=12, day=31),
        datetime(year=2022, month=1, day=1),
        datetime(year=2022, month=1, day=2),
    ]

    actual = reporting_window.get_dates()

    assert actual == expected


def test_get_overflow_dates_returns_list_of_datetimes_within_cutoff_period():
    start_datetime = datetime(year=2019, month=12, day=30, hour=0, minute=0, second=0)
    end_datetime = datetime(year=2019, month=12, day=31, hour=0, minute=0, second=0)
    conversation_cutoff = timedelta(days=3)

    reporting_window = ReportingWindow(start_datetime, end_datetime, conversation_cutoff)

    expected_overflow_dates = [
        datetime(year=2019, month=12, day=31),
        datetime(year=2020, month=1, day=1),
        datetime(year=2020, month=1, day=2),
    ]

    actual = reporting_window.get_overflow_dates()

    assert actual == expected_overflow_dates


@pytest.mark.parametrize(
    "test_case",
    [
        ({"date": datetime(2021, 1, 31, tzinfo=tzutc()), "expected": False}),
        ({"date": datetime(2021, 2, 1, tzinfo=tzutc()), "expected": False}),
        ({"date": datetime(2021, 2, 19, 0, 0, 1, tzinfo=tzutc()), "expected": True}),
        ({"date": datetime(2021, 2, 20, tzinfo=tzutc()), "expected": True}),
        ({"date": datetime(2021, 2, 28, tzinfo=tzutc()), "expected": True}),
        ({"date": datetime(2021, 2, 28, 0, 0, 1, tzinfo=tzutc()), "expected": False}),
        ({"date": datetime(2021, 3, 1, tzinfo=tzutc()), "expected": False}),
    ],
)
def test_contains_returns_correct_boolean(test_case):
    start_datetime = datetime(
        year=2021, month=2, day=19, hour=0, minute=0, second=0, tzinfo=tzutc()
    )
    end_datetime = datetime(year=2021, month=2, day=28, hour=0, minute=0, second=0, tzinfo=tzutc())
    conversation_cutoff = timedelta(days=3)

    reporting_window = ReportingWindow(start_datetime, end_datetime, conversation_cutoff)

    actual = reporting_window.contains(test_case["date"])

    assert actual == test_case["expected"]
