from gp2gp.domain.spine.models import Message, ParsedConversation
from tests.builders.common import a_string, a_datetime


def build_parsed_conversation(**kwargs):
    return ParsedConversation(
        id=kwargs.get("id", a_string(36)),
        request_started=kwargs.get("request_started", build_message()),
        request_completed=kwargs.get("request_completed", build_message()),
        intermediate_messages=kwargs.get("intermediate_messages", []),
        request_completed_ack=kwargs.get("request_completed_ack", build_message()),
    )


def build_message(**kwargs):
    return Message(
        time=kwargs.get("time", a_datetime()),
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        guid=kwargs.get("guid", a_string(36)),
        interaction_id=kwargs.get("interaction_id", a_string(17)),
        from_party_asid=kwargs.get("from_party_asid", a_string(6)),
        to_party_asid=kwargs.get("to_party_asid", a_string(6)),
        message_ref=kwargs.get("message_ref", None),
        error_code=kwargs.get("error_code", None),
    )


def build_spine_item(**kwargs):
    return {
        "_time": kwargs.get("time", a_datetime().isoformat()),
        "conversationID": kwargs.get("conversation_id", a_string(36)),
        "GUID": kwargs.get("guid", a_string(36)),
        "interactionID": kwargs.get("interaction_id", a_string(17)),
        "messageSender": kwargs.get("message_sender", a_string(6)),
        "messageRecipient": kwargs.get("message_recipient", a_string(6)),
        "messageRef": kwargs.get("message_ref", "NotProvided"),
        "jdiEvent": kwargs.get("jdi_event", "NONE"),
        "_raw": kwargs.get("raw", ""),
    }
