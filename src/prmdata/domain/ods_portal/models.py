from dataclasses import dataclass
from datetime import datetime
from typing import List
from dateutil import parser


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


def construct_organisation_list_from_dict(data: dict) -> OrganisationMetadata:
    return OrganisationMetadata(
        generated_on=parser.isoparse(data["generated_on"]),
        practices=[
            PracticeDetails(asids=p["asids"], ods_code=p["ods_code"], name=p["name"])
            for p in data["practices"]
        ],
        ccgs=[CcgDetails(ods_code=c["ods_code"], name=c["name"]) for c in data["ccgs"]],
    )
