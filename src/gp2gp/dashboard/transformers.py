from datetime import datetime
from typing import Iterable

from gp2gp.dashboard.models import ServiceDashboardData, PracticeSummary
from gp2gp.service.models import PracticeSlaMetrics


def construct_service_dashboard_data(
    sla_metrics: Iterable[PracticeSlaMetrics],
) -> ServiceDashboardData:
    return ServiceDashboardData(
        generated_on=datetime.now(), practices=[PracticeSummary(ods="A12345")]
    )
