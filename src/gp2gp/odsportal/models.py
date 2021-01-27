from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class OrganisationDetails:
    asid: str
    ods_code: str
    name: str


@dataclass
class OrganisationMetadata:
    generated_on: datetime
    practices: List[OrganisationDetails]
    ccgs: List[OrganisationDetails]
