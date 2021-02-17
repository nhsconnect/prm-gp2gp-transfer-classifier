from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List
from enum import Enum


ERROR_SUPPRESSED = 15


class TransferStatus(Enum):
    PENDING = "PENDING"
    INTEGRATED = "INTEGRATED"
    FAILED = "FAILED"


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice_asid: str
    sending_practice_asid: str
    final_error_code: Optional[int]
    intermediate_error_codes: List[int]
    status: TransferStatus
    date_requested: datetime
    date_completed: Optional[datetime]
