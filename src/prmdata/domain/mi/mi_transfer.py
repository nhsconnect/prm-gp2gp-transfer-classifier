from dataclasses import dataclass
from typing import List, Optional

from prmdata.domain.mi.event_type import EventType


@dataclass
class MiPractice:
    supplier: Optional[str]  # deduced by subsequent events
    ods_code: Optional[str]


@dataclass
class EventSummary:
    event_generated_datetime: str
    event_type: EventType
    event_id: str


@dataclass
class MiTransfer:
    conversation_id: str
    events: List[EventSummary]
    requesting_practice: MiPractice
    sending_practice: MiPractice
    slow_transfer: Optional[bool] = None
