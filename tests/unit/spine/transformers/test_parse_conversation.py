import pytest

from gp2gp.domain.spine.models import (
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
    APPLICATION_ACK,
    COMMON_POINT_TO_POINT,
    Conversation,
    ParsedConversation,
)
from gp2gp.domain.spine.transformers import parse_conversation, ConversationMissingStart
from tests.builders.spine import build_message
from tests.builders.common import a_string


def test_parses_a_complete_conversation():
    request_started_message = build_message(interaction_id=EHR_REQUEST_STARTED)
    request_completed_message = build_message(guid="abc-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK)
    request_completed_ack_message = build_message(
        interaction_id=APPLICATION_ACK,
        message_ref="abc-1",
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
        request_completed_ack_message,
    ]

    conversation = Conversation("abc-0", messages)

    expected = ParsedConversation(
        id="abc-0",
        request_started=request_started_message,
        request_completed=request_completed_message,
        intermediate_messages=[request_started_ack_message],
        request_completed_ack=request_completed_ack_message,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_throws_conversation_missing_start_exception_when_conversation_start_omitted():
    request_completed_ack_message = build_message(interaction_id=APPLICATION_ACK)
    messages = [request_completed_ack_message]

    conversation = Conversation("a-conversation-id", messages)

    with pytest.raises(ConversationMissingStart):
        parse_conversation(conversation)


def test_parses_incomplete_conversation():
    request_started_message = build_message(interaction_id=EHR_REQUEST_STARTED)
    request_completed_message = build_message(interaction_id=EHR_REQUEST_COMPLETED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK)

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
    ]

    conversation = Conversation("abc-0", messages)

    expected = ParsedConversation(
        id="abc-0",
        request_started=request_started_message,
        request_completed=request_completed_message,
        intermediate_messages=[request_started_ack_message],
        request_completed_ack=None,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_parses_conversation_with_large_messages():
    request_started_message = build_message(interaction_id=EHR_REQUEST_STARTED)
    request_completed_message = build_message(guid="abc-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK)
    common_p2p_message = build_message(guid="abc-2", interaction_id=COMMON_POINT_TO_POINT)
    common_p2p_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref="abc-2")
    request_completed_ack_message = build_message(
        interaction_id=APPLICATION_ACK,
        message_ref="abc-1",
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
        common_p2p_message,
        common_p2p_ack_message,
        request_completed_ack_message,
    ]

    conversation = Conversation("abc-0", messages)

    expected = ParsedConversation(
        id="abc-0",
        request_started=request_started_message,
        request_completed=request_completed_message,
        intermediate_messages=[
            request_started_ack_message,
            common_p2p_message,
            common_p2p_ack_message,
        ],
        request_completed_ack=request_completed_ack_message,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_parses_conversation_without_request_completed():
    guid = a_string()
    request_started_message = build_message(guid=guid, interaction_id=EHR_REQUEST_STARTED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref=guid)

    messages = [
        request_started_message,
        request_started_ack_message,
    ]

    conversation = Conversation(guid, messages)

    expected = ParsedConversation(
        id=guid,
        request_started=request_started_message,
        request_completed=None,
        intermediate_messages=[request_started_ack_message],
        request_completed_ack=None,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_saves_the_last_message_when_multiple_request_completed_messages_or_final_acks():
    request_started_message = build_message(guid="cde", interaction_id=EHR_REQUEST_STARTED)
    request_completed_message_1 = build_message(guid="cde-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_message_2 = build_message(guid="cde-2", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_message_3 = build_message(guid="cde-3", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_ack_message_1 = build_message(
        interaction_id=APPLICATION_ACK, message_ref="cde-3", error_code=12
    )
    request_completed_ack_message_2 = build_message(
        interaction_id=APPLICATION_ACK, message_ref="cde-3", error_code=None
    )

    messages = [
        request_started_message,
        request_completed_message_1,
        request_completed_message_2,
        request_completed_message_3,
        request_completed_ack_message_1,
        request_completed_ack_message_2,
    ]

    conversation = Conversation("cde", messages)

    expected = ParsedConversation(
        id="cde",
        request_started=request_started_message,
        request_completed=request_completed_message_3,
        intermediate_messages=[],
        request_completed_ack=request_completed_ack_message_2,
    )
    actual = parse_conversation(conversation)

    assert actual == expected
