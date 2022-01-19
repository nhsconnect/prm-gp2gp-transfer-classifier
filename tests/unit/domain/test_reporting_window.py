from datetime import datetime, timedelta
from typing import List

import pytest
from dateutil.tz import UTC, gettz
from freezegun import freeze_time

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


def test_returns_empty_list_given_cutoff_of_0():
    start_datetime = datetime(year=2022, month=1, day=12, hour=0, minute=0, second=0, tzinfo=UTC)
    end_datetime = datetime(year=2022, month=1, day=13, hour=0, minute=0, second=0, tzinfo=UTC)

    reporting_window = ReportingWindow(
        start_datetime, end_datetime, conversation_cutoff=timedelta(days=0)
    )

    expected_overflow_dates: List = []

    actual = reporting_window.get_overflow_dates()

    assert actual == expected_overflow_dates


def test_throws_value_error_given_end_datetime_but_no_start_datetime():
    end_datetime = datetime(year=2019, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)
    conversation_cutoff = timedelta(days=3)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=None, end_datetime=end_datetime, conversation_cutoff=conversation_cutoff
        )
    assert str(e.value) == "Start datetime must be provided if end datetime is provided"


def test_throws_value_error_given_start_datetime_but_no_end_datetime():
    start_datetime = datetime(year=2019, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)
    conversation_cutoff = timedelta(days=3)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=start_datetime,
            end_datetime=None,
            conversation_cutoff=conversation_cutoff,
        )
    assert str(e.value) == "End datetime must be provided if start datetime is provided"


def test_throws_value_error_given_start_datetime_is_after_end_datetime():
    start_datetime = datetime(year=2019, month=12, day=2, hour=0, minute=0, second=0, tzinfo=UTC)
    end_datetime = datetime(year=2019, month=12, day=1, hour=0, minute=0, second=0, tzinfo=UTC)
    conversation_cutoff = timedelta(days=3)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            conversation_cutoff=conversation_cutoff,
        )
    assert str(e.value) == "Start datetime must be before end datetime"


@pytest.mark.parametrize(
    "start_hour, end_hour",
    [(12, 0), (0, 12)],
)
def test_throws_value_error_given_datetimes_that_are_not_midnight(start_hour, end_hour):
    start_datetime = datetime(
        year=2019, month=12, day=1, hour=start_hour, minute=0, second=0, tzinfo=UTC
    )
    end_datetime = datetime(
        year=2019, month=12, day=2, hour=end_hour, minute=0, second=0, tzinfo=UTC
    )
    conversation_cutoff = timedelta(days=3)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            conversation_cutoff=conversation_cutoff,
        )
    assert str(e.value) == "Datetime must be at midnight"


@pytest.mark.parametrize(
    "cutoff_days, expected_dates",
    [
        (0, [datetime(year=2020, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)]),
        (1, [datetime(year=2020, month=12, day=30, hour=0, minute=0, second=0, tzinfo=UTC)]),
        (14, [datetime(year=2020, month=12, day=17, hour=0, minute=0, second=0, tzinfo=UTC)]),
    ],
)
@freeze_time(datetime(year=2021, month=1, day=1, hour=3, minute=0, second=0, tzinfo=UTC))
def test_returns_dates_list_from_yesterday_midnight_minus_cutoff_when_datetimes_are_none(
    cutoff_days, expected_dates
):
    reporting_window = ReportingWindow(
        start_datetime=None, end_datetime=None, conversation_cutoff=timedelta(days=cutoff_days)
    )

    actual_dates = reporting_window.get_dates()

    assert actual_dates == expected_dates


@freeze_time(
    datetime(year=2021, month=5, day=1, hour=3, minute=0, second=0, tzinfo=UTC).astimezone(
        gettz("Europe/London")
    )
)
def test_returns_dates_list_when_today_time_is_bst():
    reporting_window = ReportingWindow(
        start_datetime=None, end_datetime=None, conversation_cutoff=timedelta(days=0)
    )

    actual_dates = reporting_window.get_dates()
    expected_dates = [datetime(year=2021, month=4, day=30, hour=0, minute=0, second=0, tzinfo=UTC)]

    assert actual_dates == expected_dates


@pytest.mark.parametrize(
    "cutoff_days, expected_overflow_dates",
    [
        (0, []),
        (1, [datetime(year=2020, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)]),
        (
            3,
            [
                datetime(year=2020, month=12, day=29, hour=0, minute=0, second=0, tzinfo=UTC),
                datetime(year=2020, month=12, day=30, hour=0, minute=0, second=0, tzinfo=UTC),
                datetime(year=2020, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC),
            ],
        ),
    ],
)
@freeze_time(datetime(year=2021, month=1, day=1, hour=3, minute=0, second=0, tzinfo=UTC))
def test_returns_list_of_overflow_dates_depending_on_cutoff_when_start_and_end_datetime_are_none(
    cutoff_days, expected_overflow_dates
):
    reporting_window = ReportingWindow(
        start_datetime=None, end_datetime=None, conversation_cutoff=timedelta(days=cutoff_days)
    )

    actual_dates = reporting_window.get_overflow_dates()

    assert actual_dates == expected_overflow_dates
