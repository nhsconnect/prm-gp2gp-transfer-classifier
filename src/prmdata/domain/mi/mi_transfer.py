from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class MiPractice:
    supplier: Optional[str]  # deduced by subsequent events
    ods_code: str


@dataclass
class EventSummary:
    event_generated_datetime: datetime
    event_type: str
    event_id: str


@dataclass
class MiTransfer:
    conversation_id: str
    events: List[EventSummary]
    requesting_practice: MiPractice
    sending_practice: MiPractice
