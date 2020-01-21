from datetime import datetime

from gp2gp.models.spine import Conversation, Message
from gp2gp.transformers.spine import transform_messages_to_conversations


A_DATETIME = datetime(year=2020, month=6, day=6)
A_GUID = "C80F9DC0-10F5-11EA-A8DF-A33D4E28615A"
AN_INTERACTION_ID = "COPC_IN000001UK01"
AN_ODS_CODE = "P82023"


def _build_message(conversation_id=None, time=None):
    return Message(
        time=A_DATETIME if time is None else time,
        conversation_id=A_GUID if conversation_id is None else conversation_id,
        guid=A_GUID,
        interaction_id=AN_INTERACTION_ID,
        from_party_ods=AN_ODS_CODE,
        to_party_ods=AN_ODS_CODE,
        message_ref=A_GUID,
        error_code=None,
    )


def test_transform_messages_to_conversations_produces_correct_number_of_conversations():
    message_one = _build_message(conversation_id="abc")
    message_two = _build_message(conversation_id="xyz")
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one]), Conversation("xyz", [message_two])]

    actual = transform_messages_to_conversations(messages)

    assert list(actual) == expected


def test_transform_messages_to_conversations_produces_correct_number_of_messages_within_conversations():
    message_one = _build_message(conversation_id="abc")
    message_two = _build_message(conversation_id="abc")
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one, message_two])]

    actual = transform_messages_to_conversations(messages)

    assert list(actual) == expected


def test_transform_messages_to_conversations_sorts_messages_within_conversations():
    message_one = _build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    message_two = _build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=5))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_two, message_one])]

    actual = transform_messages_to_conversations(messages)

    assert list(actual) == expected
