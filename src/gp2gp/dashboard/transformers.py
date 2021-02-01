from datetime import datetime
from typing import Iterable

from dateutil.tz import tzutc

from gp2gp.dashboard.models import (
    ServiceDashboardData,
    PracticeSummary,
    MonthlyMetrics,
    RequesterMetrics,
    TimeToIntegrateSla,
    ServiceDashboardMetadata,
    OrganisationDetails,
)
from gp2gp.odsportal.models import OrganisationMetadata
from gp2gp.service.models import PracticeSlaMetrics


def construct_service_dashboard_data(
    sla_metrics: Iterable[PracticeSlaMetrics], year: int, month: int
) -> ServiceDashboardData:

    return ServiceDashboardData(
        generated_on=datetime.now(tzutc()),
        practices=[
            PracticeSummary(
                ods_code=practice.ods_code,
                name=practice.name,
                metrics=[
                    MonthlyMetrics(
                        year=year,
                        month=month,
                        requester=RequesterMetrics(
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


def construct_service_dashboard_metadata(
    organisation_metadata: OrganisationMetadata,
) -> ServiceDashboardMetadata:
    return ServiceDashboardMetadata(
        generated_on=organisation_metadata.generated_on,
        practices=[
            OrganisationDetails(ods_code=practice.ods_code, name=practice.name)
            for practice in organisation_metadata.practices
        ],
        ccgs=[
            OrganisationDetails(ods_code=ccg.ods_code, name=ccg.name)
            for ccg in organisation_metadata.ccgs
        ],
    )
