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


def fetch_practice_data(client=requests, url=ODS_PORTAL_SEARCH_URL, params=SEARCH_PARAMS):
    next_page = "Next-Page"
    response = client.get(url, params)
    practice_data = json.loads(response.content)["Organisations"]

    while next_page in response.headers:
        response = client.get(response.headers[next_page])
        practice_data += json.loads(response.content)["Organisations"]

    return practice_data


def construct_practice_list(response: List[dict]):
    return [PracticeDetails(ods_code=p["OrgId"], name=p["Name"]) for p in response]
