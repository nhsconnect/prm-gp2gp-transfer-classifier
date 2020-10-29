import json
from datetime import datetime
from typing import Iterable

import requests
from dateutil.tz import tzutc
from dateutil import parser

from gp2gp.odsportal.models import OrganisationDetails, OrganisationMetadata

ODS_PORTAL_SEARCH_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"
PRACTICE_SEARCH_PARAMS = {
    "PrimaryRoleId": "RO177",
    "Status": "Active",
    "NonPrimaryRoleId": "RO76",
    "Limit": "1000",
}
CCG_SEARCH_PARAMS = {
    "PrimaryRoleId": "RO98",
    "Status": "Active",
    "Limit": "1000",
}

NEXT_PAGE_HEADER = "Next-Page"


class OdsPortalException(Exception):
    def __init__(self, message, status_code):
        super(OdsPortalException, self).__init__(message)
        self.status_code = status_code


class OdsDataFetcher:
    def __init__(self, client=requests, search_url=ODS_PORTAL_SEARCH_URL):
        self._search_url = search_url
        self._client = client

    def fetch_organisation_data(self, params):
        response_data = list(self._iterate_organisation_data(params))
        return response_data

    def _iterate_organisation_data(self, params):
        response = self._client.get(self._search_url, params)
        yield from self._process_practice_data_response(response)

        while NEXT_PAGE_HEADER in response.headers:
            response = self._client.get(response.headers[NEXT_PAGE_HEADER])
            yield from self._process_practice_data_response(response)

    @classmethod
    def _process_practice_data_response(cls, response):
        if response.status_code != 200:
            raise OdsPortalException("Unable to fetch organisation data", response.status_code)
        return json.loads(response.content)["Organisations"]


def construct_organisation_list_from_dict(data: dict) -> OrganisationMetadata:
    return OrganisationMetadata(
        generated_on=parser.isoparse(data["generated_on"]),
        practices=[
            OrganisationDetails(ods_code=p["ods_code"], name=p["name"]) for p in data["practices"]
        ],
        ccgs=[OrganisationDetails(ods_code=c["ods_code"], name=c["name"]) for c in data["ccgs"]],
    )


def construct_organisation_metadata_from_ods_portal_response(
    practiceData: Iterable[dict],
    ccgData: Iterable[dict],
) -> OrganisationMetadata:
    unique_practices = _remove_duplicated_organisations(practiceData)
    unique_ccgs = _remove_duplicated_organisations(ccgData)

    return OrganisationMetadata(
        generated_on=datetime.now(tzutc()),
        practices=[
            OrganisationDetails(ods_code=p["OrgId"], name=p["Name"]) for p in unique_practices
        ],
        ccgs=[OrganisationDetails(ods_code=c["OrgId"], name=c["Name"]) for c in unique_ccgs],
    )


def _remove_duplicated_organisations(raw_organisations: Iterable[dict]) -> Iterable[dict]:
    return {obj["OrgId"]: obj for obj in raw_organisations}.values()
