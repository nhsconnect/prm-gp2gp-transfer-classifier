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
class SicblMetadata:
    ods_code: str
    name: str
    practices: List[str]


@dataclass
class OrganisationMetadata:
    generated_on: datetime
    year: int
    month: int
    practices: List[PracticeMetadata]
    sicbls: List[SicblMetadata]

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
            sicbls=[
                SicblMetadata(
                    ods_code=sicbl["ods_code"], name=sicbl["name"], practices=sicbl["practices"]
                )
                for sicbl in data["sicbls"]
            ],
        )
