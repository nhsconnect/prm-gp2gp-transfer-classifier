from tests.builders.spine import build_parsed_conversation, build_message


def test_extracts_sending_practice_asid():
    conversation = build_parsed_conversation(
        request_started=build_message(to_party_asid="121212121212")
    )

    actual = conversation.sending_practice_asid()

    expected = "121212121212"

    assert actual == expected


def test_extracts_requesting_practice_asid():
    conversation = build_parsed_conversation(
        request_started=build_message(from_party_asid="123456789012")
    )

    actual = conversation.requesting_practice_asid()

    expected = "123456789012"

    assert actual == expected


def test_extracts_requesting_supplier():
    conversation = build_parsed_conversation(request_started=build_message(from_system="EMIS"))

    actual = conversation.requesting_supplier()

    expected = "EMIS"

    assert actual == expected


def test_extracts_sending_supplier():
    conversation = build_parsed_conversation(request_started=build_message(to_system="Vision"))

    actual = conversation.sending_supplier()

    expected = "Vision"

    assert actual == expected


def test_extracts_final_error_codes():
    conversation = build_parsed_conversation(
        request_completed_ack_messages=[
            build_message(error_code=99),
            build_message(error_code=1),
            build_message(error_code=None),
        ]
    )

    actual = conversation.final_error_codes()

    expected = [99, 1, None]

    assert actual == expected


def test_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversation = build_parsed_conversation(request_completed_ack_messages=[])

    actual = conversation.final_error_codes()

    expected = []

    assert actual == expected


def test_extracts_sender_error_code_when_no_sender_error():
    conversation = build_parsed_conversation(request_started_ack=build_message(error_code=None))

    actual = conversation.sender_error()

    expected = None

    assert actual == expected


def test_extracts_sender_error_code_when_sender_error():
    conversation = build_parsed_conversation(request_started_ack=build_message(error_code=10))

    actual = conversation.sender_error()

    expected = 10

    assert actual == expected
