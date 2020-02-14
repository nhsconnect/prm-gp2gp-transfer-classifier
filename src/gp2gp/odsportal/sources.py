import json
import requests


ODS_PORTAL_SEARCH_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"
SEARCH_PARAMS = {
    "PrimaryRoleId": "RO177",
    "Status": "Active",
    "NonPrimaryRoleId": "RO76",
    "Limit": "1000",
}


def fetch_practice_data(client=requests, url=ODS_PORTAL_SEARCH_URL, params=SEARCH_PARAMS):
    response = client.get(url, params)
    return json.loads(response.content)["Organisations"]
