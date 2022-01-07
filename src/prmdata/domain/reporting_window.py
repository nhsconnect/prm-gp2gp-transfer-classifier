from datetime import datetime
from typing import List

from prmdata.utils.date_converter import date_range_to_dates_converter


class ReportingWindow:
    def __init__(self, start_datetime: datetime, end_datetime: datetime):
        self._dates = date_range_to_dates_converter(start_datetime, end_datetime)

    def get_dates(self) -> List[datetime]:
        return self._dates
