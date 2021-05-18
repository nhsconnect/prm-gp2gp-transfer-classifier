from tests.builders.spine import build_parsed_conversation, build_message


def test_extracts_sending_practice_asid():
    conversation = build_parsed_conversation(
        request_started=build_message(to_party_asid="121212121212")
    )

    actual = conversation.sending_practice_asid()

    expected = "121212121212"

    assert actual == expected


def test_extracts_requesting_practice_asid():
    conversations = build_parsed_conversation(
        request_started=build_message(from_party_asid="123456789012")
    )

    actual = conversations.requesting_practice_asid()

    expected = "123456789012"

    assert actual == expected
