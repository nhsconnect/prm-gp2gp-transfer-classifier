import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases


def test_extracts_sender_error_code_when_no_sender_error():
    conversation = Gp2gpConversation(messages=test_cases.request_acknowledged_successfully())

    actual = conversation.sender_error()

    expected = None

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_acknowledged_with_error,
        test_cases.core_ehr_sent_with_sender_error,
    ],
)
def test_extracts_sender_error_code_when_sender_error(test_case):
    conversation = Gp2gpConversation(messages=test_case(error_code=10))

    actual = conversation.sender_error()

    expected = 10

    assert actual == expected
