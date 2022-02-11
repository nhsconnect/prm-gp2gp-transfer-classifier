from typing import List

from prmdata.domain.ods_portal.organisation_metadata import CcgDetails, PracticeDetails


class OrganisationLookup:
    def __init__(self, practices: List[PracticeDetails], ccgs: List[CcgDetails]):
        self._asid_to_practice_ods_mapping = {
            asid: practice.ods_code for practice in practices for asid in practice.asids
        }
        self._ods_to_ccg_ods_mapping = {
            practice_ods_code: ccg.ods_code for ccg in ccgs for practice_ods_code in ccg.practices
        }

    def has_asid_code(self, asid: str):
        return asid in self._asid_to_practice_ods_mapping

    def practice_ods_code_from_asid(self, asid: str):
        return self._asid_to_practice_ods_mapping.get(asid)

    def ccg_ods_code_from_practice_ods_code(self, ods_code: str):
        return self._ods_to_ccg_ods_mapping.get(ods_code)
