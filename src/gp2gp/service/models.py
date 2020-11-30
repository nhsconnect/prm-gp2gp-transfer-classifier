from datetime import timedelta
from typing import NamedTuple, Optional, List
from enum import Enum, auto

ERROR_SUPPRESSED = 15


class TransferStatus(Enum):
    REQUESTED = auto()


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice_ods_code: str
    sending_practice_ods_code: str
    final_error_code: Optional[int]
    intermediate_error_codes: List[int]
    status: TransferStatus
    pending: bool


class SuccessfulTransfer(NamedTuple):
    conversation_id: str
    sla_duration: timedelta
    requesting_practice_ods_code: str
    sending_practice_ods_code: str


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
