from datetime import datetime
from typing import List, NamedTuple


class TimeToIntegrateSla(NamedTuple):
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


class RequestorMetrics(NamedTuple):
    time_to_integrate_sla: TimeToIntegrateSla


class MonthlyMetrics(NamedTuple):
    year: int
    month: int
    requestor: RequestorMetrics


class PracticeSummary(NamedTuple):
    ods: str
    metrics: List[MonthlyMetrics]


class ServiceDashboardData(NamedTuple):
    generated_on: datetime
    practices: List[PracticeSummary]
