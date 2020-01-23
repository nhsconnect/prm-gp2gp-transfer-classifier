from datetime import timedelta
from typing import NamedTuple


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: timedelta
    requesting_practice_ods: str
    sending_practice_ods: str
