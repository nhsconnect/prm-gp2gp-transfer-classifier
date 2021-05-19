from typing import List, Iterable

from prmdata.domain.ods_portal.models import PracticeDetails


class PracticeLookup:
    def __init__(self, practices: List[PracticeDetails]):
        self._practices = practices

    def all_practices(self) -> Iterable[PracticeDetails]:
        return iter(self._practices)

    def has_asid_code(self, asid):
        return asid in (code for practice in self._practices for code in practice.asids)
