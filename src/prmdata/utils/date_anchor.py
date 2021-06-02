from datetime import datetime

from dateutil.relativedelta import relativedelta


class DateAnchor:
    def __init__(self, now: datetime):
        self._now = now
        self._a_month_ago = now - relativedelta(months=1)

    @property
    def current_month_prefix(self) -> str:
        return f"{self._now.year}/{self._now.month}"

    @property
    def previous_month_prefix(self) -> str:
        return f"{self._a_month_ago.year}/{self._a_month_ago.month}"
