from gp2gp.spine.models import Message, ParsedConversation
from tests.builders.common import a_string, a_datetime


def build_parsed_conversation(**kwargs):
    return ParsedConversation(
        id=kwargs.get("id", a_string(36)),
        request_started=kwargs.get("request_started", build_message()),
        request_completed=kwargs.get("request_completed", build_message()),
        request_completed_ack=kwargs.get("request_completed_ack", build_message()),
    )


def build_message(**kwargs):
    return Message(
        time=kwargs.get("time", a_datetime()),
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        guid=kwargs.get("guid", a_string(36)),
        interaction_id=kwargs.get("interaction_id", a_string(17)),
        from_party_ods_code=kwargs.get("from_party_ods_code", a_string(6)),
        to_party_ods_code=kwargs.get("to_party_ods_code", a_string(6)),
        message_ref=kwargs.get("message_ref", None),
        error_code=kwargs.get("error_code", None),
    )


SPINE_CSV_FIELDS = [
    "_time",
    "conversationID",
    "GUID",
    "interactionID",
    "fromNACS",
    "toNACS",
    "messageRef",
    "jdiEvent",
    "_raw",
]


def build_spine_csv_row(**kwargs):
    return [
        kwargs.get("time", a_datetime().isoformat()),
        kwargs.get("conversation_id", a_string(36)),
        kwargs.get("guid", a_string(36)),
        kwargs.get("interaction_id", a_string(17)),
        kwargs.get("from_nacs", a_string(6)),
        kwargs.get("to_nacs", a_string(6)),
        kwargs.get("message_ref", "NotProvided"),
        kwargs.get("jdi_event", "NONE"),
        kwargs.get("raw", ""),
    ]
