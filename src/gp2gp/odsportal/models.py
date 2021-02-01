from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class CcgDetails:
    ods_code: str
    name: str


@dataclass
class PracticeDetails:
    ods_code: str
    name: str
    asids: List[str]


@dataclass
class OrganisationMetadata:
    generated_on: datetime
    practices: List[PracticeDetails]
    ccgs: List[CcgDetails]
