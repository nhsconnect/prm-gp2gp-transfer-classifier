from tests.builders.spine import build_gp2gp_conversation, build_message


def test_extracts_requesting_supplier():
    conversation = build_gp2gp_conversation(request_started=build_message(from_system="EMIS"))

    actual = conversation.requesting_supplier()

    expected = "EMIS"

    assert actual == expected


def test_extracts_sending_supplier():
    conversation = build_gp2gp_conversation(request_started=build_message(to_system="Vision"))

    actual = conversation.sending_supplier()

    expected = "Vision"

    assert actual == expected
