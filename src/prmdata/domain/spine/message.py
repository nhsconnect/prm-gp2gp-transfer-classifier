from datetime import datetime
from typing import NamedTuple, Optional, Iterable, Iterator

from dateutil import parser


class Message(NamedTuple):
    time: datetime
    conversation_id: str
    guid: str
    interaction_id: str
    from_party_asid: str
    to_party_asid: str
    message_ref: Optional[str]
    error_code: Optional[int]
    from_system: str
    to_system: str


def _parse_error_code(error):
    return None if error == "NONE" else int(error)


def _parse_message_ref(ref):
    return None if ref == "NotProvided" else ref


def construct_messages_from_splunk_items(items: Iterable[dict]) -> Iterator[Message]:
    for item in items:
        yield Message(
            time=parser.isoparse(item["_time"]),
            conversation_id=item["conversationID"],
            guid=item["GUID"],
            interaction_id=item["interactionID"],
            from_party_asid=item["messageSender"],
            to_party_asid=item["messageRecipient"],
            message_ref=_parse_message_ref(item["messageRef"]),
            error_code=_parse_error_code(item["jdiEvent"]),
            from_system=item["fromSystem"],
            to_system=item["toSystem"],
        )
