from typing import NamedTuple
from datetime import datetime


class Message(NamedTuple):
    time: datetime
    conversation_id: str
    guid: str
