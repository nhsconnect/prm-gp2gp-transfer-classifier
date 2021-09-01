from typing import NamedTuple, List

from prmdata.domain.spine.message import Message


class Conversation(NamedTuple):
    id: str
    messages: List[Message]
