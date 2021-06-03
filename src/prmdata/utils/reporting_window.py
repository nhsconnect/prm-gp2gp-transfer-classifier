from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc


class MonthlyReportingWindow:
    @classmethod
    def prior_to(cls, date_anchor: datetime):
        overflow_month_start = datetime(date_anchor.year, date_anchor.month, 1, tzinfo=tzutc())
        metric_month_start = overflow_month_start - relativedelta(months=1)
        return cls(metric_month_start, overflow_month_start)

    def __init__(self, metric_month_start, overflow_month_start):
        self._metric_month_start = metric_month_start
        self._overflow_month_start = overflow_month_start

    @property
    def metric_month(self) -> int:
        return self._metric_month_start.month

    @property
    def metric_year(self) -> int:
        return self._metric_month_start.year

    @property
    def overflow_month(self) -> int:
        return self._overflow_month_start.month

    @property
    def overflow_year(self) -> int:
        return self._overflow_month_start.year

    def contains(self, time: datetime):
        return self._metric_month_start <= time < self._overflow_month_start
