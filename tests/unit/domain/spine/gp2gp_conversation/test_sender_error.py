from tests.builders.spine import build_gp2gp_conversation, build_message


def test_extracts_sender_error_code_when_no_sender_error():
    conversation = build_gp2gp_conversation(request_started_ack=build_message(error_code=None))

    actual = conversation.sender_error()

    expected = None

    assert actual == expected


def test_extracts_sender_error_code_when_sender_error():
    conversation = build_gp2gp_conversation(request_started_ack=build_message(error_code=10))

    actual = conversation.sender_error()

    expected = 10

    assert actual == expected
