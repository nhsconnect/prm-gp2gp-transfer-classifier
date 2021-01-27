from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class OrganisationDetails:
    ods_code: str
    name: str
    asid: str = None


@dataclass
class OrganisationMetadata:
    generated_on: datetime
    practices: List[OrganisationDetails]
    ccgs: List[OrganisationDetails]
