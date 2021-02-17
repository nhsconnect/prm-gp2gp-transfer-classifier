from typing import NamedTuple
from enum import Enum, auto


class PracticeSlaMetrics(NamedTuple):
    ods_code: str
    name: str
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


class SlaBand(Enum):
    WITHIN_3_DAYS = auto()
    WITHIN_8_DAYS = auto()
    BEYOND_8_DAYS = auto()
