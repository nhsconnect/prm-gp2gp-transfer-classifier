from datetime import datetime
from typing import NamedTuple, List


class PracticeSummary(NamedTuple):
    ods: str


class ServiceDashboardData(NamedTuple):
    generated_on: datetime
    practices: List[PracticeSummary]
