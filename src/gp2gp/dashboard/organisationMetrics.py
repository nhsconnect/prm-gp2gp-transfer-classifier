from dataclasses import dataclass
from datetime import datetime
from typing import List

from gp2gp.odsportal.models import OrganisationMetadata


@dataclass
class OrganisationDetails:
    ods_code: str
    name: str


@dataclass
class ServiceDashboardMetadata:
    generated_on: datetime
    practices: List[OrganisationDetails]
    ccgs: List[OrganisationDetails]


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