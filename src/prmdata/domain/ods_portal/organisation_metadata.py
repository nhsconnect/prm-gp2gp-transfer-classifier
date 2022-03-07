from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil import parser


@dataclass
class PracticeMetadata:
    ods_code: str
    name: str
    asids: List[str]


@dataclass
class CcgMetadata:
    ods_code: str
    name: str
    practices: List[str]


@dataclass
class OrganisationMetadata:
    generated_on: datetime
    year: int
    month: int
    practices: List[PracticeMetadata]
    ccgs: List[CcgMetadata]

    @classmethod
    def from_dict(cls, data):
        return OrganisationMetadata(
            generated_on=parser.isoparse(data["generated_on"]),
            year=data["year"],
            month=data["month"],
            practices=[
                PracticeMetadata(
                    asids=practice["asids"], ods_code=practice["ods_code"], name=practice["name"]
                )
                for practice in data["practices"]
            ],
            ccgs=[
                CcgMetadata(ods_code=ccg["ods_code"], name=ccg["name"], practices=ccg["practices"])
                for ccg in data["ccgs"]
            ],
        )
