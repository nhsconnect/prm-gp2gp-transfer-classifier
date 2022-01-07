from datetime import datetime

from prmdata.utils.date_converter import date_range_to_dates_converter
from tests.builders.common import a_datetime


def test_date_range_to_dates_converter_returns_datetimes_between_start_and_end_datetime():
    start_datetime = a_datetime(year=2021, month=2, day=1, hour=0, minute=0, second=0)
    end_datetime = a_datetime(year=2021, month=2, day=3, hour=0, minute=0, second=0)

    actual = date_range_to_dates_converter(start_datetime, end_datetime)

    expected = [
        datetime(year=2021, month=2, day=1, hour=0, minute=0, second=0),
        datetime(year=2021, month=2, day=2, hour=0, minute=0, second=0),
    ]

    assert actual == expected
