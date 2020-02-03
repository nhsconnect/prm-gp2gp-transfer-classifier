from datetime import datetime
from typing import NamedTuple, List


class MonthlyMetrics(NamedTuple):
    year: int
    month: int


class PracticeSummary(NamedTuple):
    ods: str
    metrics: List[MonthlyMetrics]


class ServiceDashboardData(NamedTuple):
    generated_on: datetime
    practices: List[PracticeSummary]
