from datetime import datetime, timedelta
from typing import List, Optional

from prmdata.utils.date_converter import convert_date_range_to_dates


class ReportingWindow:
    def __init__(
        self,
        start_datetime: datetime,
        end_datetime: datetime,
        conversation_cutoff: Optional[timedelta],
    ):
        self._start_datetime = start_datetime
        self._end_datetime = end_datetime
        self._dates = convert_date_range_to_dates(start_datetime, end_datetime)

        cutoff_datetime = (
            end_datetime if conversation_cutoff is None else end_datetime + conversation_cutoff
        )
        self._overflow_dates = convert_date_range_to_dates(end_datetime, cutoff_datetime)

    def get_dates(self) -> List[datetime]:
        return self._dates

    def get_overflow_dates(self) -> List[datetime]:
        return self._overflow_dates
