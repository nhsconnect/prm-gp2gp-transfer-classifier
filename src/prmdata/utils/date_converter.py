from datetime import datetime, timedelta
from typing import List


def convert_date_range_to_dates(start_datetime: datetime, end_datetime: datetime) -> List[datetime]:
    if start_datetime > end_datetime:
        raise ValueError("Start datetime must be before end datetime")

    delta = end_datetime - start_datetime
    return [start_datetime + timedelta(days=days) for days in range(delta.days)]
