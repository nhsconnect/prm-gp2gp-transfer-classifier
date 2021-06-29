from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases


def test_extracts_sender_error_code_when_no_sender_error():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.request_acknowledged_successfully()
    )

    actual = conversation.sender_error()

    expected = None

    assert actual == expected


def test_extracts_sender_error_code_when_sender_error():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.request_acknowledged_with_error(error_code=10)
    )

    actual = conversation.sender_error()

    expected = 10

    assert actual == expected
