from datetime import datetime

from prmdata.spine.models import (
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
    APPLICATION_ACK,
    Conversation,
    ParsedConversation,
)
from prmdata.spine.transformers import group_into_conversations, parse_conversation
from tests.builders.spine import build_message


def test_group_into_conversations_produces_correct_conversations():
    message_one = build_message(conversation_id="abc")
    message_two = build_message(conversation_id="xyz")
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one]), Conversation("xyz", [message_two])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_group_into_conversations_produces_correct_messages_within_conversations():
    message_one = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=5))
    message_two = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one, message_two])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_group_into_conversations_sorts_messages_within_conversations():
    message_one = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    message_two = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=5))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_two, message_one])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_parse_conversation_parses_a_complete_conversation():
    request_started_message = build_message(interaction_id=EHR_REQUEST_STARTED,)
    request_completed_message = build_message(
        guid="54F949C0-DC7F-4EBC-8AE2-72BF2D0AF4EE", interaction_id=EHR_REQUEST_COMPLETED,
    )
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK,)
    request_completed_ack_message = build_message(
        interaction_id=APPLICATION_ACK, message_ref="54F949C0-DC7F-4EBC-8AE2-72BF2D0AF4EE",
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
        request_completed_ack_message,
    ]

    conversation = Conversation("F8DAFCAA-5012-427B-BDB4-354256A4874B", messages)

    expected = ParsedConversation(
        id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        request_started=request_started_message,
        request_completed=request_completed_message,
        request_completed_ack=request_completed_ack_message,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_parse_conversation_returns_none_when_start_omitted():
    request_completed_ack_message = build_message(
        interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
    )
    messages = [request_completed_ack_message]

    conversation = Conversation("F8DAFCAA-5012-427B-BDB4-354256A4874B", messages)

    expected = None

    actual = parse_conversation(conversation)

    assert actual == expected


def test_parse_conversation_parses_incomplete_conversation():
    request_started_message = build_message(
        interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK05",
    )
    request_completed_message = build_message(
        interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN030000UK06",
    )
    request_started_ack_message = build_message(
        interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
    ]

    conversation = Conversation("F8DAFCAA-5012-427B-BDB4-354256A4874B", messages)

    expected = ParsedConversation(
        id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        request_started=request_started_message,
        request_completed=request_completed_message,
        request_completed_ack=None,
    )
    actual = parse_conversation(conversation)

    assert actual == expected
