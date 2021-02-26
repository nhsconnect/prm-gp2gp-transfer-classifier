from typing import Iterator, Iterable

from dateutil import parser

from prmdata.domain.spine.models import Message


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
        )
