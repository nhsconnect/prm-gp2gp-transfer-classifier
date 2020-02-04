from datetime import datetime
from typing import Iterable

from gp2gp.dashboard.models import (
    MonthlyMetrics,
    PracticeSummary,
    RequestorMetrics,
    ServiceDashboardData,
    TimeToIntegrateSla,
)
from gp2gp.service.models import PracticeSlaMetrics


def construct_service_dashboard_data(
    sla_metrics: Iterable[PracticeSlaMetrics], year: int, month: int
) -> ServiceDashboardData:

    return ServiceDashboardData(
        generated_on=datetime.now(),
        practices=[
            PracticeSummary(
                ods=practice.ods,
                metrics=[
                    MonthlyMetrics(
                        year=year,
                        month=month,
                        requestor=RequestorMetrics(
                            time_to_integrate_sla=TimeToIntegrateSla(
                                within_3_days=practice.within_3_days,
                                within_8_days=practice.within_8_days,
                                beyond_8_days=practice.beyond_8_days,
                            )
                        ),
                    )
                ],
            )
            for practice in sla_metrics
        ],
    )
