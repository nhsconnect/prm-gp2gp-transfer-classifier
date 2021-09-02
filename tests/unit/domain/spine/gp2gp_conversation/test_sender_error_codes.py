from unittest.mock import Mock

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


def test_extracts_sender_error_codes_when_no_sender_error():
    conversation = Gp2gpConversation(
        messages=test_cases.request_acknowledged_successfully(),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.sender_error_codes()

    expected = [None]

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_acknowledged_with_error,
        test_cases.core_ehr_sent_with_sender_error,
    ],
)
def test_extracts_sender_error_codes_when_sender_error(test_case):
    conversation = Gp2gpConversation(
        messages=test_case(error_code=10), probe=mock_gp2gp_conversation_observability_probe
    )

    actual = conversation.sender_error_codes()

    expected = [10]

    assert actual == expected


def test_extracts_sender_error_codes_when_multiple_sender_errors():
    conversation = Gp2gpConversation(
        messages=test_cases.multiple_sender_acknowledgements(error_codes=[42, 43]),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.sender_error_codes()

    expected = [42, 43]

    assert actual == expected
