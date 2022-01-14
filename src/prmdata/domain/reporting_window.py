from datetime import datetime, time, timedelta
from typing import List, Optional

from prmdata.utils.date_converter import convert_date_range_to_dates


class ReportingWindow:
    def __init__(
        self,
        start_datetime: Optional[datetime],
        end_datetime: datetime,
        conversation_cutoff: Optional[timedelta],
    ):
        self._start_datetime = start_datetime
        self._end_datetime = end_datetime

        self._validate_datetimes(start_datetime, end_datetime)

        self._dates = convert_date_range_to_dates(start_datetime, end_datetime)

        cutoff_datetime = (
            end_datetime if conversation_cutoff is None else end_datetime + conversation_cutoff
        )
        self._overflow_dates = convert_date_range_to_dates(end_datetime, cutoff_datetime)

    def _validate_datetimes(self, start_datetime: Optional[datetime], end_datetime: datetime):
        if start_datetime is None:
            raise ValueError("Start datetime must be provided if end datetime is provided")

        self._validate_datetime_is_at_midnight(start_datetime)
        self._validate_datetime_is_at_midnight(end_datetime)

    @staticmethod
    def _validate_datetime_is_at_midnight(a_datetime: datetime):
        midnight = time(hour=0, minute=0, second=0)
        if a_datetime and a_datetime.time() != midnight:
            raise ValueError("Datetime must be at midnight")

    def get_dates(self) -> List[datetime]:
        return self._dates

    def get_overflow_dates(self) -> List[datetime]:
        return self._overflow_dates
