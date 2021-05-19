from tests.builders.spine import build_parsed_conversation, build_message


def test_returns_none_when_request_has_been_made():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=None,
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected
