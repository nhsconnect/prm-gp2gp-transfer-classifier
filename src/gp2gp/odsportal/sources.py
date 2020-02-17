import json
from typing import List

import requests

from gp2gp.odsportal.models import PracticeDetails

ODS_PORTAL_SEARCH_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"
SEARCH_PARAMS = {
    "PrimaryRoleId": "RO177",
    "Status": "Active",
    "NonPrimaryRoleId": "RO76",
    "Limit": "1000",
}


class OdsPortalException(Exception):
    def __init__(self, message, status_code):
        super(OdsPortalException, self).__init__(message)
        self.status_code = status_code


def fetch_practice_data(client, url=ODS_PORTAL_SEARCH_URL, params=SEARCH_PARAMS):
    next_page = "Next-Page"
    response = client.get(url, params)
    if response.status_code != 200:
        raise OdsPortalException("Unable to fetch practice data", response.status_code)

    practice_data = json.loads(response.content)["Organisations"]

    while next_page in response.headers:
        response = client.get(response.headers[next_page])
        practice_data += json.loads(response.content)["Organisations"]

    return practice_data


def construct_practice_list(response: List[dict]):
    return [PracticeDetails(ods_code=p["OrgId"], name=p["Name"]) for p in response]


def get_practice_list(client=requests):
    practice_data = fetch_practice_data(client)
    return construct_practice_list(practice_data)
