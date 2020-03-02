from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class TimeToIntegrateSla:
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class RequesterMetrics:
    time_to_integrate_sla: TimeToIntegrateSla


@dataclass
class MonthlyMetrics:
    year: int
    month: int
    requester: RequesterMetrics


@dataclass
class PracticeSummary:
    ods_code: str
    name: str
    metrics: List[MonthlyMetrics]


@dataclass
class ServiceDashboardData:
    generated_on: datetime
    practices: List[PracticeSummary]
