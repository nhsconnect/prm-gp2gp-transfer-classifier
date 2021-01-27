from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List
from enum import Enum, auto

ERROR_SUPPRESSED = 15


class TransferStatus(Enum):
    PENDING = auto()
    INTEGRATED = auto()
    FAILED = auto()


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice_asid: str
    sending_practice_asid: str
    final_error_code: Optional[int]
    intermediate_error_codes: List[int]
    status: TransferStatus
    date_completed: Optional[datetime]


class SlaBand(Enum):
    WITHIN_3_DAYS = auto()
    WITHIN_8_DAYS = auto()
    BEYOND_8_DAYS = auto()


class PracticeSlaMetrics(NamedTuple):
    ods_code: str
    name: str
    within_3_days: int
    within_8_days: int
    beyond_8_days: int
