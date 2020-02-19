import json
from typing import List

import requests

from gp2gp.odsportal.models import PracticeDetails

ODS_PORTAL_SEARCH_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"
DEFAULT_SEARCH_PARAMS = {
    "PrimaryRoleId": "RO177",
    "Status": "Active",
    "NonPrimaryRoleId": "RO76",
    "Limit": "1000",
}


class OdsPortalException(Exception):
    def __init__(self, message, status_code):
        super(OdsPortalException, self).__init__(message)
        self.status_code = status_code


class OdsPracticeDataFetcher:
    def __init__(self, client=requests, search_url=ODS_PORTAL_SEARCH_URL):
        self._search_url = search_url
        self._client = client

    def fetch_practice_data(self, params=None):
        if params is None:
            params = DEFAULT_SEARCH_PARAMS
        next_page = "Next-Page"
        response = self._client.get(self._search_url, params)

        if response.status_code != 200:
            raise OdsPortalException("Unable to fetch practice data", response.status_code)

        practice_data = []
        practice_data += json.loads(response.content)["Organisations"]

        while next_page in response.headers:
            response = self._client.get(response.headers[next_page])
            practice_data += json.loads(response.content)["Organisations"]

        return practice_data


def construct_practice_list(data_fetcher=None) -> List[PracticeDetails]:
    if data_fetcher is None:
        data_fetcher = OdsPracticeDataFetcher()
    response = data_fetcher.fetch_practice_data()
    return [PracticeDetails(ods_code=p["OrgId"], name=p["Name"]) for p in response]
