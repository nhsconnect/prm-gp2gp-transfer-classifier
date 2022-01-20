from datetime import datetime

import pytest
from dateutil.tz import UTC

from prmdata.utils.date_converter import (
    convert_date_range_to_dates,
    convert_to_datetime_string,
    convert_to_datetimes_string,
)
from tests.builders.common import a_datetime


def test_returns_datetimes_between_start_and_end_datetime():
    start_datetime = a_datetime(year=2021, month=2, day=1, hour=0, minute=0, second=0)
    end_datetime = a_datetime(year=2021, month=2, day=3, hour=0, minute=0, second=0)

    actual = convert_date_range_to_dates(start_datetime, end_datetime)

    expected = [
        datetime(year=2021, month=2, day=1, hour=0, minute=0, second=0, tzinfo=UTC),
        datetime(year=2021, month=2, day=2, hour=0, minute=0, second=0, tzinfo=UTC),
    ]

    assert actual == expected


def test_throws_exception_given_start_datetime_is_after_end_datetime():
    start_datetime = datetime(year=2021, month=2, day=1, hour=0, minute=0, second=0)
    end_datetime = datetime(year=2021, month=1, day=31, hour=0, minute=0, second=0)

    with pytest.raises(ValueError) as e:
        convert_date_range_to_dates(start_datetime, end_datetime)

    assert str(e.value) == "Start datetime must be before end datetime"


def test_convert_to_datetime_string_returns_datetime_string_given_a_datetime():
    some_datetime = a_datetime(year=2021, month=11, day=13, hour=2, minute=0, second=0)

    actual = convert_to_datetime_string(some_datetime)

    expected = "2021-11-13T02:00:00+00:00"

    assert actual == expected


def test_convert_to_datetime_string_returns_none_string_given_no_datetime():
    actual = convert_to_datetime_string(a_datetime=None)

    expected = "None"

    assert actual == expected


def test_convert_to_datetimes_string_returns_datetimes_string_given_datetimes():
    some_datetime = a_datetime(year=2021, month=11, day=13, hour=2, minute=0, second=0)
    some_other_datetime = a_datetime(year=2022, month=10, day=13, hour=2, minute=0, second=0)

    actual = convert_to_datetimes_string([some_datetime, some_other_datetime])

    expected = ["2021-11-13T02:00:00+00:00", "2022-10-13T02:00:00+00:00"]

    assert actual == expected
