from typing import List, Iterable

from prmdata.domain.ods_portal.models import PracticeDetails


class PracticeLookup:
    def __init__(self, practices: List[PracticeDetails]):
        self._practices = practices

    def all_practices(self) -> Iterable[PracticeDetails]:
        return iter(self._practices)
