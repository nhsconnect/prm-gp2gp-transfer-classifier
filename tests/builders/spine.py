from unittest.mock import Mock

from prmdata.domain.spine.message import Message
from tests.builders.common import a_datetime, a_string


def build_mock_gp2gp_conversation(**kwargs):
    conversation_id = kwargs.get("conversation_id", a_string())
    final_ack_time = kwargs.get("final_acknowledgement_time", a_datetime())
    req_completed_time = kwargs.get("request_completed_time", a_datetime())
    sending_practice_asid = kwargs.get("sending_practice_asid", a_string())
    requesting_practice_asid = kwargs.get("requesting_practice_asid", a_string())

    conversation = Mock()
    conversation.conversation_id.return_value = conversation_id
    conversation.effective_final_acknowledgement_time.return_value = final_ack_time
    conversation.effective_request_completed_time.return_value = req_completed_time
    conversation.sending_practice_asid.return_value = sending_practice_asid
    conversation.requesting_practice_asid.return_value = requesting_practice_asid

    return conversation


def build_message(**kwargs) -> Message:
    return Message(
        time=kwargs.get("time", a_datetime()),
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        guid=kwargs.get("guid", a_string(36)),
        interaction_id=kwargs.get("interaction_id", a_string(17)),
        from_party_asid=kwargs.get("from_party_asid", a_string(6)),
        to_party_asid=kwargs.get("to_party_asid", a_string(6)),
        message_ref=kwargs.get("message_ref", None),
        error_code=kwargs.get("error_code", None),
        to_system=kwargs.get("to_system", a_string(4)),
        from_system=kwargs.get("from_system", a_string(4)),
    )


def build_spine_item(**kwargs):
    spine_item = {
        "_time": kwargs.get("time", a_datetime().isoformat()),
        "conversationID": kwargs.get("conversation_id", a_string(36)),
        "GUID": kwargs.get("guid", a_string(36)),
        "interactionID": kwargs.get("interaction_id", a_string(17)),
        "messageSender": kwargs.get("message_sender", a_string(6)),
        "messageRecipient": kwargs.get("message_recipient", a_string(6)),
        "messageRef": kwargs.get("message_ref", "NotProvided"),
        "jdiEvent": kwargs.get("jdi_event", "NONE"),
    }

    if kwargs.get("to_system"):
        spine_item["toSystem"] = kwargs.get("to_system")
    if kwargs.get("from_system"):
        spine_item["fromSystem"] = kwargs.get("from_system")

    return spine_item
