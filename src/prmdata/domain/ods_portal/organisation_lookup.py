from typing import List, Optional, Tuple

from prmdata.domain.ods_portal.organisation_metadata import CcgMetadata, PracticeMetadata

YearNumber = int
MonthNumber = int
YearMonth = Tuple[YearNumber, MonthNumber]


class OrganisationLookup:
    def __init__(
        self, practices: List[PracticeMetadata], ccgs: List[CcgMetadata], year_month: YearMonth
    ):
        self._asid_to_practice_ods_mapping = {
            asid: practice.ods_code for practice in practices for asid in practice.asids
        }
        self._ods_to_ccg_ods_mapping = {
            practice_ods_code: ccg.ods_code for ccg in ccgs for practice_ods_code in ccg.practices
        }
        self._asid_to_practice_name_mapping = {
            asid: practice.name for practice in practices for asid in practice.asids
        }
        self._ods_to_ccg_name_mapping = {
            practice_ods_code: ccg.name for ccg in ccgs for practice_ods_code in ccg.practices
        }
        self._year_month = year_month

    def has_asid_code(self, asid: str) -> bool:
        return asid in self._asid_to_practice_ods_mapping

    def practice_ods_code_from_asid(self, asid: str) -> Optional[str]:
        return self._asid_to_practice_ods_mapping.get(asid)

    def practice_name_from_asid(self, asid: str) -> Optional[str]:
        return self._asid_to_practice_name_mapping.get(asid)

    def ccg_name_from_practice_ods_code(self, ods_code: str) -> Optional[str]:
        return self._ods_to_ccg_name_mapping.get(ods_code)

    def ccg_ods_code_from_practice_ods_code(self, ods_code: str) -> Optional[str]:
        return self._ods_to_ccg_ods_mapping.get(ods_code)

    @property
    def year(self) -> int:
        return self._year_month[0]

    @property
    def month(self) -> int:
        return self._year_month[1]
