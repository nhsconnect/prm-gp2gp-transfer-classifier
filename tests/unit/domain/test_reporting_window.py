from datetime import datetime, timedelta
from typing import List

from dateutil.tz import UTC

from prmdata.domain.reporting_window import ReportingWindow


def test_get_dates_returns_list_of_datetimes_within_reporting_window():
    start_datetime = datetime(year=2021, month=12, day=30, tzinfo=UTC)
    end_datetime = datetime(year=2022, month=1, day=3, tzinfo=UTC)
    conversation_cutoff = timedelta(days=14)

    reporting_window = ReportingWindow(start_datetime, end_datetime, conversation_cutoff)

    expected = [
        datetime(year=2021, month=12, day=30, tzinfo=UTC),
        datetime(year=2021, month=12, day=31, tzinfo=UTC),
        datetime(year=2022, month=1, day=1, tzinfo=UTC),
        datetime(year=2022, month=1, day=2, tzinfo=UTC),
    ]

    actual = reporting_window.get_dates()

    assert actual == expected


def test_get_overflow_dates_returns_list_of_datetimes_within_cutoff_period():
    start_datetime = datetime(year=2019, month=12, day=30, hour=0, minute=0, second=0, tzinfo=UTC)
    end_datetime = datetime(year=2019, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)
    conversation_cutoff = timedelta(days=3)

    reporting_window = ReportingWindow(start_datetime, end_datetime, conversation_cutoff)

    expected_overflow_dates = [
        datetime(year=2019, month=12, day=31, tzinfo=UTC),
        datetime(year=2020, month=1, day=1, tzinfo=UTC),
        datetime(year=2020, month=1, day=2, tzinfo=UTC),
    ]

    actual = reporting_window.get_overflow_dates()

    assert actual == expected_overflow_dates


def test_returns_empty_list_given_0_cutoff():
    start_datetime = datetime(year=2022, month=1, day=12, hour=0, minute=0, second=0, tzinfo=UTC)
    end_datetime = datetime(year=2022, month=1, day=13, hour=0, minute=0, second=0, tzinfo=UTC)
    conversation_cutoff = timedelta(days=0)

    reporting_window = ReportingWindow(start_datetime, end_datetime, conversation_cutoff)

    expected_overflow_dates: List = []

    actual = reporting_window.get_overflow_dates()

    assert actual == expected_overflow_dates
