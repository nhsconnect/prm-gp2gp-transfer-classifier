from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil import parser


@dataclass
class PracticeDetails:
    ods_code: str
    name: str
    asids: List[str]


@dataclass
class CcgDetails:
    ods_code: str
    name: str
    practices: List[str]


@dataclass
class OrganisationMetadata:
    generated_on: datetime
    practices: List[PracticeDetails]
    ccgs: List[CcgDetails]

    @classmethod
    def from_dict(cls, data):
        return OrganisationMetadata(
            generated_on=parser.isoparse(data["generated_on"]),
            practices=[
                PracticeDetails(
                    asids=practice["asids"], ods_code=practice["ods_code"], name=practice["name"]
                )
                for practice in data["practices"]
            ],
            ccgs=[
                CcgDetails(ods_code=ccg["ods_code"], name=ccg["name"], practices=ccg["practices"])
                for ccg in data["ccgs"]
            ],
        )
