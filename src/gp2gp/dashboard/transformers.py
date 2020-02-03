from datetime import datetime
from typing import Iterable

from gp2gp.dashboard.models import ServiceDashboardData, PracticeSummary, MonthlyMetrics
from gp2gp.service.models import PracticeSlaMetrics


def construct_service_dashboard_data(
    sla_metrics: Iterable[PracticeSlaMetrics], year: int, month: int
) -> ServiceDashboardData:

    return ServiceDashboardData(
        generated_on=datetime.now(),
        practices=[
            PracticeSummary(ods=practice.ods, metrics=[MonthlyMetrics(year=year, month=month)])
            for practice in sla_metrics
        ],
    )
