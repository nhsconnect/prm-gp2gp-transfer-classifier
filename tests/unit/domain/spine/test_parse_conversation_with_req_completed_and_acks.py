from prmdata.domain.spine.parsed_conversation import (
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
    APPLICATION_ACK,
    parse_conversation,
    COMMON_POINT_TO_POINT,
)
from prmdata.domain.spine.conversation import Conversation
from tests.builders.spine import build_message


def test_returns_a_list_of_request_completed_messages():
    request_started_message = build_message(guid="abc", interaction_id=EHR_REQUEST_STARTED)
    request_completed_message = build_message(guid="abc-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_message_2 = build_message(guid="abc-2", interaction_id=EHR_REQUEST_COMPLETED)
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref="abc")
    request_completed_ack_message = build_message(
        interaction_id=APPLICATION_ACK, message_ref="abc-1"
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_completed_message_2,
        request_started_ack_message,
        request_completed_ack_message,
    ]

    conversation = Conversation("abc-0", messages)

    actual = parse_conversation(conversation)

    assert actual.request_completed_messages == [
        request_completed_message,
        request_completed_message_2,
    ]


def test_returns_a_list_of_request_completed_acks():
    request_started_message = build_message(guid="abc", interaction_id=EHR_REQUEST_STARTED)
    request_completed_message = build_message(guid="abc-1", interaction_id=EHR_REQUEST_COMPLETED)
    request_completed_message_2 = build_message(guid="abc-2", interaction_id=EHR_REQUEST_COMPLETED)
    common_point_to_point = build_message(guid="abc-3", interaction_id=COMMON_POINT_TO_POINT)
    common_point_to_point_ack = build_message(
        interaction_id=COMMON_POINT_TO_POINT, message_ref="abc-3"
    )
    request_started_ack_message = build_message(interaction_id=APPLICATION_ACK, message_ref="abc")
    request_completed_ack_message = build_message(
        interaction_id=APPLICATION_ACK, message_ref="abc-2", error_code=12
    )
    request_completed_ack_message_2 = build_message(
        interaction_id=APPLICATION_ACK, message_ref="abc-1", error_code=None
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_completed_message_2,
        common_point_to_point,
        common_point_to_point_ack,
        request_started_ack_message,
        request_completed_ack_message,
        request_completed_ack_message_2,
    ]

    conversation = Conversation("abc-0", messages)

    actual = parse_conversation(conversation)

    assert actual.request_completed_ack_codes == [12, None]
