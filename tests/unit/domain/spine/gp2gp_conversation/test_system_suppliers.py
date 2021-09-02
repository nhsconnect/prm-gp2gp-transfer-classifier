from unittest.mock import Mock

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


def test_extracts_requesting_supplier():
    conversation = Gp2gpConversation(
        messages=test_cases.request_made(requesting_system="A System"),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.requesting_supplier()

    expected = "A System"

    assert actual == expected


def test_extracts_sending_supplier():
    conversation = Gp2gpConversation(
        messages=test_cases.request_made(sending_system="Another System"),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.sending_supplier()

    expected = "Another System"

    assert actual == expected
