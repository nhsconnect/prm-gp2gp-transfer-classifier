from gp2gp.spine.models import (
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
    APPLICATION_ACK,
    COMMON_POINT_TO_POINT,
    Conversation,
    ParsedConversation,
)
from gp2gp.spine.transformers import parse_conversation
from tests.builders.spine import build_message


def test_parse_conversation_parses_a_complete_conversation():
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
        request_completed_ack=request_completed_ack_message,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_parse_conversation_returns_none_when_start_omitted():
    request_completed_ack_message = build_message(interaction_id=APPLICATION_ACK)
    messages = [request_completed_ack_message]

    conversation = Conversation("a-conversation-id", messages)

    expected = None

    actual = parse_conversation(conversation)

    assert actual == expected


def test_parse_conversation_parses_incomplete_conversation():
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
        request_completed_ack=None,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_parse_conversation_parses_conversation_with_large_messages():
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
        request_completed_ack=request_completed_ack_message,
    )
    actual = parse_conversation(conversation)

    assert actual == expected
