from typing import List

from prmdata.domain.ods_portal.organisation_metadata import PracticeDetails


class OrganisationLookup:
    def __init__(self, practices: List[PracticeDetails]):
        self._practices = practices
        self._asid_to_practice_ods_mapping = {
            asid: practice.ods_code for practice in practices for asid in practice.asids
        }

    def has_asid_code(self, asid: str):
        return asid in self._asid_to_practice_ods_mapping

    def practice_ods_code_from_asid(self, asid: str):
        return self._asid_to_practice_ods_mapping.get(asid)
