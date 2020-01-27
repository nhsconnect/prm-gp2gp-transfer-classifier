from datetime import timedelta
from typing import NamedTuple, Optional

ERROR_SUPPRESSED = 15


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice_ods: str
    sending_practice_ods: str
    error_code: Optional[int]
    pending: bool
