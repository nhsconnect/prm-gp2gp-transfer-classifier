from datetime import datetime, timedelta
from typing import List, Optional


def convert_date_range_to_dates(start_datetime: datetime, end_datetime: datetime) -> List[datetime]:
    if start_datetime > end_datetime:
        raise ValueError("Start datetime must be before end datetime")

    delta = end_datetime - start_datetime
    return [start_datetime + timedelta(days=days) for days in range(delta.days)]


def convert_to_datetime_string(a_datetime: Optional[datetime]) -> str:
    return a_datetime.isoformat() if a_datetime else "None"


def convert_to_datetimes_string(datetimes: List[datetime]) -> List[str]:
    return [convert_to_datetime_string(a_datetime) for a_datetime in datetimes]
