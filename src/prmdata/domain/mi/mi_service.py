from dataclasses import dataclass
from typing import List


@dataclass
class MiMessage:
    conversation_id: str
    event_id: str


class MiService:
    @staticmethod
    def construct_mi_messages_from_mi_events(mi_events: List[dict]) -> List[MiMessage]:
        return [
            MiMessage(conversation_id=event["conversationId"], event_id=event["eventId"])
            for event in mi_events
        ]
