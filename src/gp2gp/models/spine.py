from typing import NamedTuple, Optional
from datetime import datetime


class Message(NamedTuple):
    time: datetime
    conversation_id: str
    guid: str
    interaction_id: str
    from_party_ods: str
    to_party_ods: str
    message_ref: str
    error_code: Optional[int]
