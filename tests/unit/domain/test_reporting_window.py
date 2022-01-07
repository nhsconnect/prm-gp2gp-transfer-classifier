from datetime import datetime

from prmdata.domain.reporting_window import ReportingWindow


def test_get_dates_returns_list_of_datetimes_within_reporting_window():
    start_datetime = datetime(year=2021, month=12, day=30)
    end_datetime = datetime(year=2022, month=1, day=3)

    reporting_window = ReportingWindow(start_datetime, end_datetime)

    expected = [
        datetime(year=2021, month=12, day=30),
        datetime(year=2021, month=12, day=31),
        datetime(year=2022, month=1, day=1),
        datetime(year=2022, month=1, day=2),
    ]

    actual = reporting_window.get_dates()

    assert actual == expected
