import pytest

from prmdata.domain.spine.gp2gp_conversation import (
    Gp2gpConversation,
    ConversationMissingStart,
    SpineConversationParser,
)
from prmdata.domain.spine.message import (
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
    APPLICATION_ACK,
)
from tests.builders import test_cases
from tests.builders.common import a_string
from tests.builders.spine import build_message


def test_parses_a_complete_conversation():
    request_started_message = build_message(
        conversation_id="abc", guid="abc", interaction_id=EHR_REQUEST_STARTED
    )
    request_completed_message = build_message(guid="abc-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref="abc")
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

    expected = Gp2gpConversation(
        id="abc",
        request_started=request_started_message,
        request_started_ack=request_started_ack_message,
        request_completed_messages=[request_completed_message],
        copc_continue=None,
        copc_messages=[],
        copc_ack_messages=[],
        request_completed_ack_messages=[request_completed_ack_message],
    )
    actual = SpineConversationParser(messages).parse()

    assert actual == expected


def test_throws_conversation_missing_start_exception_when_conversation_start_omitted():
    request_completed_ack_message = build_message(interaction_id=APPLICATION_ACK)
    messages = [request_completed_ack_message]

    with pytest.raises(ConversationMissingStart):
        SpineConversationParser(messages).parse()


def test_parses_incomplete_conversation():
    request_started_message = build_message(
        conversation_id="abc", guid="abc", interaction_id=EHR_REQUEST_STARTED
    )
    request_completed_message = build_message(interaction_id=EHR_REQUEST_COMPLETED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref="abc")

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
    ]

    expected = Gp2gpConversation(
        id="abc",
        request_started=request_started_message,
        request_completed_messages=[request_completed_message],
        request_started_ack=request_started_ack_message,
        copc_continue=None,
        copc_messages=[],
        copc_ack_messages=[],
        request_completed_ack_messages=[],
    )
    actual = SpineConversationParser(messages).parse()

    assert actual == expected


def test_parses_conversation_with_large_messages():
    gp2gp_messages = test_cases.successful_integration_with_large_messages()

    (
        request_started_message,
        request_started_ack_message,
        request_completed_message,
        *intermediate_messages,
        request_completed_ack_message,
    ) = gp2gp_messages
    (
        copc_continue,
        copc_1,
        copc_2,
        copc_1_ack,
        copc_2_ack,
        copc_3,
        copc_3_ack,
    ) = intermediate_messages

    expected = Gp2gpConversation(
        id=request_started_message.conversation_id,
        request_started=request_started_message,
        request_started_ack=request_started_ack_message,
        request_completed_messages=[request_completed_message],
        copc_continue=copc_continue,
        copc_messages=[copc_1, copc_2, copc_3],
        copc_ack_messages=[copc_1_ack, copc_2_ack, copc_3_ack],
        request_completed_ack_messages=[request_completed_ack_message],
    )
    actual = SpineConversationParser(gp2gp_messages).parse()

    assert actual == expected


def test_parses_conversation_without_request_completed():
    guid = a_string()
    request_started_message = build_message(
        conversation_id=guid, guid=guid, interaction_id=EHR_REQUEST_STARTED
    )
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref=guid)

    messages = [
        request_started_message,
        request_started_ack_message,
    ]

    expected = Gp2gpConversation(
        id=guid,
        request_started=request_started_message,
        request_started_ack=request_started_ack_message,
        request_completed_messages=[],
        copc_continue=None,
        copc_messages=[],
        copc_ack_messages=[],
        request_completed_ack_messages=[],
    )
    actual = SpineConversationParser(messages).parse()

    assert actual == expected


def test_saves_all_request_completed_messages_and_all_final_acks():
    request_started_message = build_message(
        conversation_id="cde", guid="cde", interaction_id=EHR_REQUEST_STARTED
    )
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref="cde")
    request_completed_message_1 = build_message(guid="cde-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_message_2 = build_message(guid="cde-2", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_message_3 = build_message(guid="cde-3", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_ack_message_1 = build_message(
        interaction_id=APPLICATION_ACK, message_ref="cde-3", error_code=12
    )
    request_completed_ack_message_2 = build_message(
        interaction_id=APPLICATION_ACK, message_ref="cde-3", error_code=None
    )
    request_completed_ack_message_3 = build_message(
        interaction_id=APPLICATION_ACK, message_ref="cde-2", error_code=None
    )

    messages = [
        request_started_message,
        request_started_ack_message,
        request_completed_message_1,
        request_completed_message_2,
        request_completed_message_3,
        request_completed_ack_message_1,
        request_completed_ack_message_2,
        request_completed_ack_message_3,
    ]

    expected = Gp2gpConversation(
        id="cde",
        request_started=request_started_message,
        request_started_ack=request_started_ack_message,
        request_completed_messages=[
            request_completed_message_1,
            request_completed_message_2,
            request_completed_message_3,
        ],
        copc_continue=None,
        copc_messages=[],
        copc_ack_messages=[],
        request_completed_ack_messages=[
            request_completed_ack_message_1,
            request_completed_ack_message_2,
            request_completed_ack_message_3,
        ],
    )
    actual = SpineConversationParser(messages).parse()

    assert actual == expected
