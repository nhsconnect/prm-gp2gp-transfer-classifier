from datetime import datetime

from dateutil.relativedelta import relativedelta


class DateAnchor:
    def __init__(self, now: datetime):
        self._now = now
        self._a_month_ago = now - relativedelta(months=1)

    def current_month_prefix(self, separator: str = "/") -> str:
        return f"{self._now.year}{separator}{self._now.month}"

    def previous_month_prefix(self, separator: str = "/") -> str:
        return f"{self._a_month_ago.year}{separator}{self._a_month_ago.month}"
