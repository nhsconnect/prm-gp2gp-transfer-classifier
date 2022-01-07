from datetime import datetime, timedelta
from typing import List


def date_range_to_dates_converter(
    start_datetime: datetime, end_datetime: datetime
) -> List[datetime]:
    delta = end_datetime - start_datetime
    return [start_datetime + timedelta(days=days) for days in range(delta.days)]
