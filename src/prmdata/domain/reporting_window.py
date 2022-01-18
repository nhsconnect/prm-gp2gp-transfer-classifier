from datetime import datetime, time, timedelta
from typing import List, Optional

from prmdata.utils.date_converter import convert_date_range_to_dates


class ReportingWindow:
    def __init__(
        self,
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
        conversation_cutoff: timedelta,
    ):
        self._start_datetime = start_datetime
        self._end_datetime = end_datetime

        self._validate_datetimes(start_datetime, end_datetime)

        if self._start_datetime is None:
            self._start_datetime = (
                self._calculate_yesterday_midnight_datetime() - conversation_cutoff
            )

        if self._end_datetime is None:
            self._end_datetime = (
                self._calculate_yesterday_midnight_datetime()
                - conversation_cutoff
                + timedelta(days=1)
            )

        self._dates = convert_date_range_to_dates(self._start_datetime, self._end_datetime)

        cutoff_datetime = self._end_datetime + conversation_cutoff
        self._overflow_dates = convert_date_range_to_dates(self._end_datetime, cutoff_datetime)

    def _validate_datetimes(
        self, start_datetime: Optional[datetime], end_datetime: Optional[datetime]
    ):
        if start_datetime is None and end_datetime:
            raise ValueError("Start datetime must be provided if end datetime is provided")
        if end_datetime is None and start_datetime:
            raise ValueError("End datetime must be provided if start datetime is provided")
        if start_datetime:
            self._validate_datetime_is_at_midnight(start_datetime)
        if end_datetime:
            self._validate_datetime_is_at_midnight(end_datetime)

    @staticmethod
    def _validate_datetime_is_at_midnight(a_datetime: datetime):
        midnight = time(hour=0, minute=0, second=0)
        if a_datetime and a_datetime.time() != midnight:
            raise ValueError("Datetime must be at midnight")

    @staticmethod
    def _calculate_yesterday_midnight_datetime() -> datetime:
        today_midnight_datetime = datetime.combine(datetime.utcnow().date(), time.min)
        return today_midnight_datetime - timedelta(days=1)

    def get_dates(self) -> List[datetime]:
        return self._dates

    def get_overflow_dates(self) -> List[datetime]:
        return self._overflow_dates
